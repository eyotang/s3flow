package rg

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3Search(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	fmt.Fprint(gz, "skip\r\nError [1]\r\nError [1]")
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	reads := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			switch r.URL.Path {
			case "/bucket/logs":
				w.WriteHeader(403)
			case "/bucket/logs/a.log.gz", "/bucket/broken.gz", "/bucket/long.log", "/bucket/a b.log":
				w.WriteHeader(200)
			default:
				w.WriteHeader(404)
			}
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			if r.URL.Query().Get("prefix") != "logs/" && r.URL.Query().Get("prefix") != "empty/" {
				t.Errorf("incorrect prefix: %s", r.URL.RawQuery)
				w.WriteHeader(400)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("prefix") == "empty/" {
				fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
				return
			}
			if r.URL.Query().Get("continuation-token") == "" {
				fmt.Fprint(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>logs/ignored.tmp</Key></Contents></ListBucketResult>")
			} else {
				fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>logs/a.log.gz</Key></Contents></ListBucketResult>")
			}
			return
		}
		mu.Lock()
		reads[r.URL.Path]++
		mu.Unlock()
		switch r.URL.Path {
		case "/bucket/logs/a.log.gz":
			w.Write(compressed.Bytes())
		case "/bucket/logs/ignored.tmp":
			fmt.Fprint(w, "temporary")
		case "/bucket/broken.gz":
			fmt.Fprint(w, "invalid gzip data")
		case "/bucket/long.log":
			fmt.Fprint(w, strings.Repeat("x", maxLine+1))
		case "/bucket/a b.log":
			fmt.Fprint(w, "ERROR")
		default:
			t.Errorf("unexpected GET %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	run := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		args = append([]string{"--Endpoint", server.URL, "--Region", "us-east-1", "--ak", "test", "--sk", "test"}, args...)
		code := Run(args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}
	code, out, diag := run("-Fin", "-g", "!*.tmp", "-e", "[1]", "-e", "never", "s3://bucket/logs", "s3://bucket/logs/a.log.gz")
	if code != 0 || diag != "" || out != "s3://bucket/logs/a.log.gz:2:Error [1]\ns3://bucket/logs/a.log.gz:3:Error [1]\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out, diag)
	}
	mu.Lock()
	count := reads["/bucket/logs/a.log.gz"]
	filtered := reads["/bucket/logs/ignored.tmp"]
	mu.Unlock()
	if filtered != 0 {
		t.Fatalf("filtered object read %d times", filtered)
	}
	if count != 1 {
		t.Fatalf("deduplicated object read %d times", count)
	}
	cases := []struct {
		args      []string
		code      int
		out, diag string
	}{
		{[]string{"-c", "Error", "s3://bucket/logs/a.log.gz"}, 0, "2\n", ""},
		{[]string{"-Fc", "-e", "[1]", "-e", "skip", "s3://bucket/logs/a.log.gz"}, 0, "3\n", ""},
		{[]string{"-Fvc", "[1]", "s3://bucket/logs/a.log.gz"}, 0, "1\n", ""},
		{[]string{"-Fc", "", "s3://bucket/logs/a.log.gz"}, 0, "3\n", ""},
		{[]string{"-q", "Error", "s3://bucket/logs/a.log.gz"}, 0, "", ""},
		{[]string{"missing", "s3://bucket/logs/"}, 1, "", ""},
		{[]string{"missing", "s3://bucket/empty"}, 2, "", "s3rg:"},
		{[]string{"missing", "s3://bucket/empty/"}, 1, "", ""},
		{[]string{"-l", "ERROR", "s3://bucket/a%20b.log", "s3://bucket/broken.gz"}, 2, "s3://bucket/a%20b.log\n", "gzip"},
		{[]string{"x", "s3://bucket/long.log"}, 2, "", "s3rg:"},
		{[]string{"[", "s3://bucket/logs/"}, 2, "", "s3rg:"},
	}
	for _, tc := range cases {
		code, out, diag := run(tc.args...)
		if code != tc.code || out != tc.out || (tc.diag == "" && diag != "") || !strings.Contains(diag, tc.diag) {
			t.Errorf("%v: code=%d stdout=%q stderr=%q", tc.args, code, out, diag)
		}
	}
}

func TestQuietCancelsOtherReads(t *testing.T) {
	reading := make(chan struct{})
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/bucket/slow" {
			close(reading)
			select {
			case <-r.Context().Done():
				close(canceled)
			case <-time.After(5 * time.Second):
				t.Error("slow request was not canceled")
			}
			return
		}
		select {
		case <-reading:
			fmt.Fprintln(w, "match")
		case <-time.After(5 * time.Second):
			t.Error("second object read did not start")
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--Endpoint", server.URL, "--Region", "us-east-1", "--ak", "test", "--sk", "test", "-q", "-j", "2", "match", "s3://bucket/slow", "s3://bucket/fast"}, &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("did not observe request cancellation")
	}
}

type benchmarkClient struct {
	searchClient
	payload []byte
}

func (c benchmarkClient) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(c.payload))}, nil
}

func BenchmarkScanObject(b *testing.B) {
	text := bytes.Repeat([]byte("INFO request completed in 14ms\nERROR request timed out after 1000ms\n"), 1<<17)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(text); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}

	opts := options{count: true, matchers: []func([]byte) bool{regexp.MustCompile("^ERROR").Match}}
	for _, tc := range []struct {
		key     string
		payload []byte
	}{
		{"plain.log", text},
		{"compressed.log.gz", compressed.Bytes()},
	} {
		b.Run(tc.key, func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			b.ReportAllocs()
			client := benchmarkClient{payload: tc.payload}
			emit := func([]byte) error { return nil }
			ctx := context.Background()
			object := target{bucket: "logs", key: tc.key}
			for b.Loop() {
				matched, err := scanObject(ctx, client, object, opts, false, emit)
				if err != nil || !matched {
					b.Fatalf("matched=%t error=%v", matched, err)
				}
			}
		})
	}
}

func TestFixedMatchersPreserveTextSemantics(t *testing.T) {
	for _, tc := range []struct {
		pattern, line string
		fold, want    bool
	}{
		{"[1]", "error [1]", false, true},
		{"[1]", "error 1", false, false},
		{"汉字", "日志中的汉字", false, true},
		{"�", "malformed \xff byte", false, true},
		{"k", "K", true, true},
	} {
		opts := options{literal: true, ignoreCase: tc.fold, patterns: []string{tc.pattern}}
		if err := opts.compileMatchers(); err != nil {
			t.Fatal(err)
		}
		if got := opts.matchers[0]([]byte(tc.line)); got != tc.want {
			t.Errorf("pattern %q, line %q, ignore case %t: got %t, want %t", tc.pattern, tc.line, tc.fold, got, tc.want)
		}
	}
}

func TestOptimizedRegexMatchesStandardLibrary(t *testing.T) {
	for _, pattern := range []string{"", "ERROR", "^ERROR", "(?m)^ERROR", "\\AERROR", "(?i)^ERROR", "ERR|WARN", "ERROR$", "a.*b", "[E]RROR", "汉字", "�", "^�"} {
		opts := options{patterns: []string{pattern}}
		if err := opts.compileMatchers(); err != nil {
			t.Fatal(err)
		}
		baseline := regexp.MustCompile(pattern)
		for _, line := range []string{"", "ERROR", "prefix ERROR", "WARN", "error", "ab", "axxb", "日志汉字", "\xff ERROR", "�"} {
			got, want := opts.matchers[0]([]byte(line)), baseline.MatchString(line)
			if got != want {
				t.Errorf("pattern %q on %q: got %t, want %t", pattern, line, got, want)
			}
		}
	}
}

func TestSearchFlushesMatchesBeforeReadingMore(t *testing.T) {
	flushed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			return
		}
		fmt.Fprint(w, "ERROR first\nERROR second\nERROR third\n")
		w.(http.Flusher).Flush()
		select {
		case <-flushed:
			fmt.Fprint(w, "ERROR fourth\n")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := NewCommand()
	cmd.SetArgs([]string{"--Endpoint", server.URL, "--Region", "us-east-1", "--ak", "test", "--sk", "test",
		"-n", "^ERROR", "s3://bucket/slow.log"})
	var output bytes.Buffer
	var once sync.Once
	cmd.SetOut(writerFunc(func(p []byte) (int, error) {
		n, err := output.Write(p)
		if strings.Contains(output.String(), "3:ERROR third\n") {
			once.Do(func() { close(flushed) })
		}
		return n, err
	}))
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}
	want := "1:ERROR first\n2:ERROR second\n3:ERROR third\n4:ERROR fourth\n"
	if got := output.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

type writerFunc func([]byte) (int, error)

func (write writerFunc) Write(p []byte) (int, error) { return write(p) }

func TestScanFlushesCompleteRecordsOnError(t *testing.T) {
	longLine := "ERROR " + strings.Repeat("x", 2*maxOutputBatch)
	client := benchmarkClient{payload: []byte("ERROR first\n" + longLine + "\nERROR third\ninvalid\x00\n")}
	opts := options{patterns: []string{"^ERROR"}, numbers: true}
	if err := opts.compileMatchers(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	found, err := scanObject(context.Background(), client, target{"logs", "app.log"}, opts, false, func(p []byte) error {
		if p[len(p)-1] != '\n' {
			t.Error("write ended with a partial record")
		}
		_, err := output.Write(p)
		return err
	})
	if !found || err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("found=%t error=%v", found, err)
	}
	if want := "1:ERROR first\n2:" + longLine + "\n3:ERROR third\n"; output.String() != want {
		t.Fatalf("output size %d, want matching records of size %d", output.Len(), len(want))
	}
}
