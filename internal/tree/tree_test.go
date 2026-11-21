package tree

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTreeListsPrefixWithoutReadingObjects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		query := r.URL.Query()
		if r.Method != http.MethodGet || strings.TrimSuffix(r.URL.Path, "/") != "/test-bucket" || query.Get("list-type") != "2" || query.Get("prefix") != "weekly logs/" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		switch query.Get("continuation-token") {
		case "":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken>
<Contents><Key>weekly logs/</Key><Size>0</Size></Contents>
<Contents><Key>weekly logs/nested/report.txt</Key><Size>8</Size></Contents>
<Contents><Key>weekly logs/log.txt</Key><Size>2</Size></Contents></ListBucketResult>`)
		case "next":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>
<Contents><Key>weekly logs/nested/old/data.json</Key><Size>1536</Size></Contents>
<Contents><Key>weekly logs/empty/</Key><Size>0</Size></Contents>
<Contents><Key>weekly logs/nested</Key><Size>0</Size></Contents>
<Contents><Key>weekly logs/tabs&#9;line.txt</Key><Size>1</Size></Contents>
<Contents><Key>weekly logs/unknown.txt</Key></Contents></ListBucketResult>`)
		default:
			t.Errorf("unexpected continuation token %q", query.Get("continuation-token"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	const plain = `s3://test-bucket/weekly%20logs/
├── empty/
├── log.txt
├── nested
├── nested/
│   ├── old/
│   │   └── data.json
│   └── report.txt
├── "tabs\tline.txt"
└── unknown.txt

3 directories, 6 objects
`
	const sized = `s3://test-bucket/weekly%20logs/
├── empty/
├── log.txt  [2 B]
├── nested  [0 B]
├── nested/
│   ├── old/
│   │   └── data.json  [1.5 KiB]
│   └── report.txt  [8 B]
├── "tabs\tline.txt"  [1 B]
└── unknown.txt  [unknown size]

3 directories, 6 objects
`
	for _, test := range []struct{ name, flag, want string }{
		{"default", "", plain},
		{"short size flag", "-s", sized},
		{"long size flag", "--size", sized},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests.Store(0)
			var out, stderr bytes.Buffer
			args := []string{"s3://test-bucket/weekly%20logs", "--Endpoint", server.URL, "--Region", "us-east-1", "--ak", "test", "--sk", "test"}
			if test.flag != "" {
				args = append(args, test.flag)
			}
			code := Run(args, &out, &stderr)
			if code != 0 || out.String() != test.want || stderr.Len() != 0 || requests.Load() != 2 {
				t.Fatalf("code=%d requests=%d stdout=%q stderr=%q", code, requests.Load(), out.String(), stderr.String())
			}
		})
	}
}

func TestTreeBucketRootAndListErrors(t *testing.T) {
	for _, failure := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query := r.URL.Query()
			if r.Method != http.MethodGet || query.Get("list-type") != "2" || query.Get("prefix") != "" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL)
			}
			w.Header().Set("Content-Type", "application/xml")
			if failure {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, "<Error><Code>AccessDenied</Code></Error>")
				return
			}
			fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
		}))
		var out, stderr bytes.Buffer
		code := Run([]string{"--Endpoint", server.URL, "--ak", "test", "--sk", "test", "s3://test-bucket"}, &out, &stderr)
		server.Close()
		if failure {
			if code != 1 || out.Len() != 0 || !strings.Contains(stderr.String(), "AccessDenied") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
			}
		} else if code != 0 || out.String() != "s3://test-bucket/\n\n0 directories, 0 objects\n" || stderr.Len() != 0 {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
		}
	}
}
