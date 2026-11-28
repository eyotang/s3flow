package objectcmd

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Exercise a 5 GiB + 1 byte object through the SDK without allocating its body.
func TestMultipartMove(t *testing.T) {
	for _, failPart := range []bool{false, true} {
		t.Run(fmt.Sprint("failPart=", failPart), func(t *testing.T) {
			var copies, aborts, completes, deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				switch {
				case r.Method == "HEAD":
					w.Header().Set("Content-Length", "5368709121")
					w.Header().Set("ETag", `"source"`)
					w.Header().Set("Content-Type", "application/zip")
					w.Header().Set("X-Amz-Meta-Project", "test")
					w.Header().Set("X-Amz-Tagging-Count", "1")
					w.Header().Set("X-Amz-Version-Id", "source/version")
				case r.Method == "GET" && q.Has("tagging"):
					if q.Get("versionId") != "source/version" {
						t.Error("wrong tag version")
					}
					fmt.Fprint(w, `<Tagging><TagSet><Tag><Key>team</Key><Value>data</Value></Tag></TagSet></Tagging>`)
				case r.Method == "POST" && q.Has("uploads"):
					if r.Header.Get("Content-Type") != "application/zip" || r.Header.Get("X-Amz-Meta-Project") != "test" || r.Header.Get("X-Amz-Tagging") != "team=data" {
						t.Errorf("lost source properties: %v", r.Header)
					}
					fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == "PUT" && q.Get("uploadId") == "u":
					copies.Add(1)
					part := q.Get("partNumber")
					want := map[string]string{"1": "bytes=0-5368709119", "2": "bytes=5368709120-5368709120"}[part]
					if want == "" || r.Header.Get("X-Amz-Copy-Source-Range") != want || r.Header.Get("X-Amz-Copy-Source-If-Match") != `"source"` ||
						r.Header.Get("X-Amz-Copy-Source") != "src/large?versionId=source%2Fversion" {
						t.Errorf("wrong copy part %s: %v", part, r.Header)
					}
					if failPart && part == "2" {
						w.WriteHeader(403)
						return
					}
					fmt.Fprintf(w, `<CopyPartResult><ETag>"part%s"</ETag></CopyPartResult>`, part)
				case r.Method == "POST" && q.Get("uploadId") == "u":
					completes.Add(1)
					body, _ := io.ReadAll(r.Body)
					if r.Header.Get("If-None-Match") != "*" || !strings.Contains(string(body), "<PartNumber>2</PartNumber>") {
						t.Errorf("bad completion: %s %v", body, r.Header)
					}
					fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
				case r.Method == "DELETE" && q.Get("uploadId") == "u":
					aborts.Add(1)
					w.WriteHeader(204)
				case r.Method == "DELETE" && r.URL.Path == "/src/large":
					if completes.Load() != 1 || r.Header.Get("If-Match") != `"source"` {
						t.Error("premature or unconditional source deletion")
					}
					deletes.Add(1)
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			expected := 0
			if failPart {
				expected = 1
			}
			cli(t, server.URL, "mv", expected, "s3://src/large", "s3://dst/large", "--part-size", "5GiB", "-j", "1")
			if copies.Load() != 2 {
				t.Fatalf("parts=%d", copies.Load())
			}
			if failPart {
				if aborts.Load() != 1 || completes.Load() != 0 || deletes.Load() != 0 {
					t.Fatal("failed upload was not aborted cleanly")
				}
			} else if aborts.Load() != 0 || completes.Load() != 1 || deletes.Load() != 1 {
				t.Fatal("did not complete and move")
			}
		})
	}
}
