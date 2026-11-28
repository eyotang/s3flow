package objectcmd

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cli(t *testing.T, endpoint, name string, wantCode int, args ...string) string {
	t.Helper()
	var out, stderr bytes.Buffer
	flags := []string{"--endpoint", endpoint, "--Region", "us-east-1", "--ak", "test", "--sk", "test-secret"}
	code := Run(name, append(flags, args...), &out, &stderr)
	if code != wantCode || (wantCode == 0 && stderr.Len() != 0) {
		t.Fatalf("s3%s %v: code %d, stdout=%q, stderr=%q", name, args, code, out.String(), stderr.String())
	}
	if code != 0 {
		return stderr.String()
	}
	return out.String()
}

func TestPresign(t *testing.T) {
	const endpoint = "https://storage.example.test"
	out := cli(t, endpoint, "presign", 0, "s3://bucket/报表/a%2Bb%20c", "--expires-in", "900", "--session-token", "temp-token")
	u, err := url.Parse(strings.TrimSpace(out))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Host != "storage.example.test" || u.Path != "/bucket/报表/a+b c" ||
		q.Get("X-Amz-Expires") != "900" || q.Get("X-Amz-SignedHeaders") != "host" ||
		q.Get("X-Amz-Security-Token") != "temp-token" || q.Get("X-Amz-Signature") == "" ||
		!strings.HasPrefix(q.Get("X-Amz-Credential"), "test/") || strings.Count(out, "\n") != 1 {
		t.Fatalf("unexpected signed URL fields: %s", out)
	}
	for _, ttl := range []string{"0", "604801"} {
		cli(t, endpoint, "presign", 2, "s3://bucket/key", "--expires-in", ttl)
	}
	cli(t, endpoint, "presign", 2, "s3://bucket")
}

func TestServerSideMoveAndConditionalDelete(t *testing.T) {
	const etag = `"original"`
	for _, scenario := range []string{"dryrun", "copy conflict", "delete conflict", "success"} {
		t.Run(scenario, func(t *testing.T) {
			var writes []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case "HEAD":
					w.Header().Set("ETag", etag)
					w.Header().Set("Content-Length", "8")
				case "PUT":
					writes = append(writes, "copy")
					if r.URL.Path != "/dst/archive/a+b c" || r.Header.Get("X-Amz-Copy-Source") != "src/data/a%2Bb%20c" ||
						r.Header.Get("X-Amz-Copy-Source-If-Match") != etag || r.Header.Get("If-None-Match") != "*" {
						t.Errorf("invalid copy %s %v", r.URL, r.Header)
					}
					if scenario == "copy conflict" {
						w.WriteHeader(412)
						return
					}
					fmt.Fprint(w, `<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
				case "DELETE":
					writes = append(writes, "delete")
					if r.URL.Path != "/src/data/a+b c" || r.Header.Get("If-Match") != etag {
						t.Errorf("unconditional or wrong delete: %s %v", r.URL, r.Header)
					}
					if scenario == "delete conflict" {
						w.WriteHeader(412)
						return
					}
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			args := []string{"s3://src/data/a%2Bb%20c", "s3://dst/archive/"}
			code, wantWrites := 0, "copy delete"
			switch scenario {
			case "dryrun":
				args = append(args, "--dryrun")
				wantWrites = ""
			case "copy conflict":
				code, wantWrites = 1, "copy"
			case "delete conflict":
				code = 1
			}
			out := cli(t, server.URL, "mv", code, args...)
			if strings.Join(writes, " ") != wantWrites {
				t.Fatalf("writes: %v", writes)
			}
			if scenario == "delete conflict" && !strings.Contains(out, "copied to s3://dst/") {
				t.Fatal(out)
			}
		})
	}
	for _, args := range [][]string{{"s3://bucket/a", "s3://bucket/a"}, {"-r", "s3://bucket/p", "s3://bucket/p/q"}, {"-r", "s3://bucket/p/q/", "s3://bucket/p/"}} {
		out := cli(t, "https://unused.invalid", "mv", 1, args...)
		if !strings.Contains(out, "same object") && !strings.Contains(out, "overlap") {
			t.Fatal(out)
		}
	}
}

func TestSyncUploadPlanAndDeleteOrdering(t *testing.T) {
	dir := t.TempDir()
	mtime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"same", "add", "update"} {
		file := filepath.Join(dir, name)
		if err := os.WriteFile(file, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(file, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	for _, scenario := range []string{"dryrun", "keep extra", "failed copy", "delete extra"} {
		t.Run(scenario, func(t *testing.T) {
			var writes []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case "GET":
					if r.URL.Query().Get("prefix") != "backup/" {
						t.Errorf("wrong prefix: %s", r.URL)
					}
					fmt.Fprint(w, `<ListBucketResult><Contents><Key>backup/update</Key><Size>6</Size><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"old"</ETag></Contents><Contents><Key>backup/same</Key><Size>4</Size><LastModified>2025-01-01T00:00:00Z</LastModified><ETag>"same"</ETag></Contents><Contents><Key>backup/extra</Key><Size>1</Size><ETag>"extra"</ETag></Contents></ListBucketResult>`)
				case "PUT":
					key := strings.TrimPrefix(r.URL.Path, "/bucket/backup/")
					writes = append(writes, key)
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != key {
						t.Errorf("unexpected upload: %q %v", body, err)
					}
					if key == "add" && r.Header.Get("If-None-Match") != "*" || key == "update" && r.Header.Get("If-Match") != `"old"` {
						t.Errorf("missing write condition: %v", r.Header)
					}
					if key == "update" && scenario == "failed copy" {
						w.WriteHeader(412)
						return
					}
					w.Header().Set("ETag", `"new"`)
				case "DELETE":
					writes = append(writes, "delete")
					if r.URL.Path != "/bucket/backup/extra" || r.Header.Get("If-Match") != `"extra"` {
						t.Errorf("unexpected delete: %s %v", r.URL, r.Header)
					}
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected request %s", r.Method)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			args := []string{dir, "s3://bucket/backup"}
			code, wantWrites := 0, "add update"
			if scenario != "keep extra" {
				args = append(args, "--delete")
			}
			switch scenario {
			case "dryrun":
				args = append(args, "--dry-run")
				wantWrites = ""
			case "failed copy":
				code = 1
			case "delete extra":
				wantWrites += " delete"
			}
			out := cli(t, server.URL, "sync", code, args...)
			if strings.Join(writes, " ") != wantWrites {
				t.Fatalf("writes=%v", writes)
			}
			if scenario == "dryrun" && (strings.Count(out, "(dryrun)") != 3 || strings.Contains(out, "/same")) {
				t.Fatal(out)
			}
		})
	}
}

func TestSyncDownloadAndRepeat(t *testing.T) {
	const content = "12345"
	const list = `<ListBucketResult><Contents><Key>dir/nested/data</Key><Size>5</Size><LastModified>2025-01-01T00:00:00Z</LastModified><ETag>"v1"</ETag></Contents></ListBucketResult>`
	var reads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			fmt.Fprint(w, list)
			return
		}
		if r.URL.Path != "/bucket/dir/nested/data" || r.Header.Get("Range") != "bytes=0-4" || r.Header.Get("If-Match") != `"v1"` {
			t.Errorf("wrong request: %s %v", r.URL, r.Header)
		}
		reads++
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Range", "bytes 0-4/5")
		w.WriteHeader(206)
		fmt.Fprint(w, content)
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "new")
	args := []string{"s3://bucket/dir/", dest}
	cli(t, server.URL, "sync", 0, append(args, "--dryrun")...)
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("dry run created a local directory")
	}
	cli(t, server.URL, "sync", 0, args...)
	file := filepath.Join(dest, "nested", "data")
	b, err := os.ReadFile(file)
	if err != nil || string(b) != content {
		t.Fatalf("downloaded %q: %v", b, err)
	}
	info, err := os.Stat(file)
	if err != nil || info.ModTime().Unix() != 1735689600 {
		t.Fatalf("timestamp: %v %v", info, err)
	}
	extra := filepath.Join(dest, "extra")
	if err := os.WriteFile(extra, []byte("extra"), 0600); err != nil {
		t.Fatal(err)
	}
	if out := cli(t, server.URL, "sync", 0, args...); out != "" {
		t.Fatalf("second sync copied files: %s", out)
	}
	cli(t, server.URL, "sync", 0, append(args, "--delete")...)
	if _, err := os.Stat(extra); !os.IsNotExist(err) || reads != 1 {
		t.Fatalf("extra stat=%v reads=%d", err, reads)
	}
}

func TestPipeAndLocalMove(t *testing.T) {
	var content []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			w.Write(content)
		case "PUT":
			if r.URL.Path == "/bucket/conflict" {
				w.WriteHeader(412)
				return
			}
			var err error
			content, err = io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			w.Header().Set("ETag", `"v1"`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	var out bytes.Buffer
	cmd := NewCommand("cp")
	cmd.SetIn(strings.NewReader("a\x00b"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--ak", "test", "--sk", "test", "--endpoint", server.URL, "-", "s3://bucket/pipe"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := cli(t, server.URL, "cp", 0, "s3://bucket/pipe", "-"); got != "a\x00b" {
		t.Fatalf("stdout %q", got)
	}
	name := filepath.Join(t.TempDir(), "record")
	if err := os.WriteFile(name, []byte("record"), 0600); err != nil {
		t.Fatal(err)
	}
	cli(t, server.URL, "mv", 1, name, "s3://bucket/conflict")
	if _, err := os.Stat(name); err != nil {
		t.Fatal(err)
	}
	cli(t, server.URL, "mv", 0, name, "s3://bucket/")
	if _, err := os.Stat(name); !os.IsNotExist(err) || string(content) != "record" {
		t.Fatalf("%v: %q", err, content)
	}
}

func TestUnsafeDownloadKeysRejectedBeforeWrites(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "2" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		fmt.Fprint(w, `<ListBucketResult><Contents><Key>p/../escape</Key><Size>1</Size></Contents></ListBucketResult>`)
	}))
	defer server.Close()
	dir := t.TempDir()
	if out := cli(t, server.URL, "mv", 1, "-r", "s3://bucket/p/", dir); !strings.Contains(out, "路径") {
		t.Fatal(out)
	}
}

func TestSyncKeepsConcurrentLocalEdit(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			fmt.Fprint(w, `<ListBucketResult><Contents><Key>p/data</Key><Size>1</Size><ETag>"v"</ETag></Contents></ListBucketResult>`)
			return
		}
		if err := os.WriteFile(file, []byte("concurrent edit"), 0600); err != nil {
			t.Error(err)
		}
		w.Header().Set("ETag", `"v"`)
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.WriteHeader(206)
		fmt.Fprint(w, "x")
	}))
	defer server.Close()
	out := cli(t, server.URL, "sync", 1, "s3://bucket/p/", dir)
	body, err := os.ReadFile(file)
	if err != nil || string(body) != "concurrent edit" || !strings.Contains(out, "changed during sync") {
		t.Fatalf("content=%q error=%v stderr=%s", body, err, out)
	}
}
