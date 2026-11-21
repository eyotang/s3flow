package rg

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type indexTestStore struct {
	sync.Mutex
	objects      map[string][]byte
	reads        []string
	ranges       []string
	writes       int
	rootConflict bool
}

func etagOf(data []byte) string { return `"` + indexChecksum(data)[:32] + `"` }

func (s *indexTestStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/")
	if r.URL.Query().Get("list-type") == "2" {
		prefix := strings.TrimSuffix(key, "/") + "/" + r.URL.Query().Get("prefix")
		var keys []string
		for candidate := range s.objects {
			if strings.HasPrefix(candidate, prefix) {
				keys = append(keys, candidate)
			}
		}
		slices.Sort(keys)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
		for _, candidate := range keys {
			data := s.objects[candidate]
			fmt.Fprint(w, "<Contents><Key>")
			xml.EscapeText(w, []byte(candidate[strings.IndexByte(candidate, '/')+1:]))
			fmt.Fprintf(w, "</Key><Size>%d</Size><ETag>%s</ETag></Contents>", len(data), etagOf(data))
		}
		fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	data, exists := s.objects[key]
	if r.Method == http.MethodPut {
		isRoot := strings.HasSuffix(key, "/.s3rg/root.json")
		if isRoot && s.rootConflict {
			s.rootConflict = false
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if (r.Header.Get("If-None-Match") == "*" && exists) ||
			(r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != etagOf(data))) ||
			(isRoot && r.Header.Get("If-Match") == "" && r.Header.Get("If-None-Match") != "*") {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.writes++
		s.objects[key] = body
		w.Header().Set("ETag", etagOf(body))
		return
	}
	if r.Method == http.MethodGet {
		s.reads = append(s.reads, key)
		if value := r.Header.Get("Range"); value != "" {
			s.ranges = append(s.ranges, value)
		}
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", etagOf(data))
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))
}

func (s *indexTestStore) dataReads() []string {
	s.Lock()
	defer s.Unlock()
	var keys []string
	for _, key := range s.reads {
		if !strings.Contains(key, "/.s3rg/") {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

func runIndexTest(t *testing.T, store *indexTestStore) func(...string) (int, string, string) {
	t.Helper()
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	return func(args ...string) (int, string, string) {
		store.Lock()
		store.reads, store.ranges, store.writes = nil, nil, 0
		store.Unlock()
		var stdout, stderr bytes.Buffer
		args = append([]string{"--Endpoint", server.URL, "--Region", "us-east-1", "--ak", "test", "--sk", "test"}, args...)
		code := Run(args, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}
}

func TestIndexBuildAndSearch(t *testing.T) {
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	fmt.Fprint(z, "only healthy requests\n")
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	store := &indexTestStore{objects: map[string][]byte{
		"bucket/logs/flat.log":       []byte("healthy request\n"),
		"bucket/logs/deep/hit.log":   []byte("request TIMEOUT\n"),
		"bucket/logs/deep/spare.log": []byte("all done\n"),
		"bucket/logs/archive.log.gz": compressed.Bytes(),
	}}
	run := runIndexTest(t, store)
	const prefix = "s3://bucket/logs/"
	const rootKey = "bucket/logs/.s3rg/root.json"
	if code, out, diag := run("--index-build", "--index-stats", prefix); code != 0 || out != "" || !strings.Contains(diag, "4 objects read") {
		t.Fatalf("build: %d %q %q", code, out, diag)
	}
	want := "s3://bucket/logs/deep/hit.log:1:request TIMEOUT\n"
	args := []string{"-Fin", "-g", "**/*.{log,gz}", "timeout", "s3://bucket/"}
	if code, out, diag := run(append([]string{"--index-stats"}, args...)...); code != 0 || out != want || !strings.Contains(diag, "3 skipped by index") {
		t.Fatalf("indexed: %d %q %q", code, out, diag)
	}
	if got := store.dataReads(); !slices.Equal(got, []string{"bucket/logs/deep/hit.log"}) || len(store.ranges) == 0 {
		t.Fatalf("data GETs %v, range requests %v", got, store.ranges)
	}
	if code, out, diag := run(append([]string{"--no-index"}, args...)...); code != 0 || out != want || diag != "" || len(store.dataReads()) != 4 {
		t.Fatalf("without index: %d %q %q, GETs %v", code, out, diag, store.dataReads())
	}
	store.Lock()
	store.objects["bucket/logs/flat.log"] = []byte("new timeout\n")
	delete(store.objects, "bucket/logs/deep/spare.log")
	store.rootConflict = true
	store.Unlock()
	if code, out, diag := run("-Fi", "--index-stats", "timeout", prefix); code != 0 || !strings.Contains(out, "new timeout") || !strings.Contains(diag, "1 not covered") {
		t.Fatalf("stale index: %d %q %q", code, out, diag)
	}
	if code, _, diag := run("--index-build", "--index-stats", prefix); code != 0 || !strings.Contains(diag, "1 objects read") {
		t.Fatalf("incremental build with conflicting write: %d %q", code, diag)
	}
	if reads := store.dataReads(); !slices.Equal(reads, []string{"bucket/logs/flat.log", "bucket/logs/flat.log"}) {
		t.Fatalf("incremental data GETs %v", reads)
	}
	if code, _, diag := run("--index-build", prefix); code != 0 || len(store.dataReads()) != 0 || store.writes != 0 {
		t.Fatalf("unchanged build: %d %q, GETs %v, PUTs %d", code, diag, store.dataReads(), store.writes)
	}
	if code, out, diag := run("-Fin", "--index-stats", "timeout", "s3://bucket/logs/deep/hit.log"); code != 0 || out != "1:request TIMEOUT\n" || !strings.Contains(diag, "0 not covered") {
		t.Fatalf("ancestor index for exact object: %d %q %q", code, out, diag)
	}
	var root indexRoot
	if err := json.Unmarshal(store.objects[rootKey], &root); err != nil || len(root.Segments) != 2 || root.Segments[0].Dead == nil {
		t.Fatalf("incremental root: %+v, error %v", root, err)
	}
	store.Lock()
	delete(store.objects, "bucket/logs/.s3rg/"+root.Segments[1].Blobs.Postings.Key)
	store.Unlock()
	if code, out, diag := run("-Fi", "timeout", prefix); code != 0 || !strings.Contains(out, "new timeout") || !strings.Contains(diag, "searching without it") || len(store.dataReads()) != 3 {
		t.Fatalf("missing posting blob: %d %q %q, GETs %v", code, out, diag, store.dataReads())
	}
	if code, _, diag := run("--index-rebuild", prefix); code != 0 || diag != "" || len(store.dataReads()) != 3 {
		t.Fatalf("rebuild: %d %q, GETs %v", code, diag, store.dataReads())
	}
}

func TestIndexPlanContainsEveryMatch(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		line    string
		wantIdx bool
	}{
		{"(?:error|warn): .+ timed out", "warn: db timed out", true},
		{"(?:error|warn): .+ timed out", "error: db timed out", true},
		{"(?i)disk", "DISK", true},
		{"(?i)ΣΣΣ", "σςσ", true},
		{"(?i)abc.*xyz", "Abc123xYZ", true},
		{"foo|fob", "fob", true},
		{"[a-c]d[ef]", "prefix bdf", true},
		{"(?:中文)+", "文档中文", true},
		{"timeout|x", "x", false},
		{"error.*�", "error \xff", true},
		{"�xx", "\xffxx", false},
		{"(?:timeout)?", "", false},
	} {
		opts := options{patterns: []string{tc.pattern}}
		if err := opts.compileMatchers(); err != nil {
			t.Fatalf("%q: %v", tc.pattern, err)
		}
		if !opts.matchers[0]([]byte(tc.line)) || (opts.gramQuery != nil) != tc.wantIdx {
			t.Fatalf("unexpected matcher or index plan for pattern %q on %q", tc.pattern, tc.line)
		}
		if opts.gramQuery == nil {
			continue
		}
		postings := make(map[uint32][]uint32)
		for i := 2; i < len(tc.line); i++ {
			postings[gramCode(tc.line[i-2], tc.line[i-1], tc.line[i])] = []uint32{0}
		}
		if !opts.gramQuery.possible(postings, 0) {
			t.Errorf("index rejected %q on %q", tc.pattern, tc.line)
		}
	}
}

func TestIndexRangeReads(t *testing.T) {
	noise := make([]byte, 128<<10)
	random := rand.New(rand.NewPCG(11, 17))
	for i := range noise {
		noise[i] = byte(32 + random.IntN(64))
	}
	store := &indexTestStore{objects: map[string][]byte{
		"bucket/logs/a.log": noise,
		"bucket/logs/b.log": []byte("z9!\n"),
	}}
	run := runIndexTest(t, store)
	const prefix = "s3://bucket/logs/"
	const indexAt = "s3://indexes/app/"
	if code, out, diag := run("--index-build", "--index", indexAt, prefix); code != 0 || out != "" || diag != "" {
		t.Fatalf("build: %d %q %q", code, out, diag)
	}
	if code, out, diag := run("--index", indexAt, "z9!", prefix); code != 0 || out != "s3://bucket/logs/b.log:z9!\n" || diag != "" {
		t.Fatalf("search using external index: %d %q %q", code, out, diag)
	}
	if reads := store.dataReads(); !slices.Equal(reads, []string{"bucket/logs/b.log"}) || len(store.ranges) != 1 || strings.HasPrefix(store.ranges[0], "bytes=0-") {
		t.Fatalf("data GETs %v, range requests %v", reads, store.ranges)
	}
	store.Lock()
	for key, data := range store.objects {
		if strings.HasSuffix(key, ".postings") {
			data[len(data)-1] ^= 1
		}
	}
	store.Unlock()
	if code, out, diag := run("--index", indexAt, "z9!", prefix); code != 0 || !strings.Contains(out, "b.log:z9!") || !strings.Contains(diag, "posting checksum") || len(store.dataReads()) != 2 {
		t.Fatalf("bad posting checksum: %d %q %q, GETs %v", code, out, diag, store.dataReads())
	}
}
