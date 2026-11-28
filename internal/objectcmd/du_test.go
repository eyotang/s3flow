package objectcmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDUGroupsAcrossPages(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		q := r.URL.Query()
		if r.Method != "GET" || strings.TrimSuffix(r.URL.Path, "/") != "/bucket" || q.Get("list-type") != "2" || q.Get("prefix") != "weekly logs/" || q.Has("delimiter") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		switch q.Get("continuation-token") {
		case "":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>two</NextContinuationToken>
<Contents><Key>weekly logs/</Key><Size>0</Size></Contents>
<Contents><Key>weekly logs/api/day</Key><Size>1024</Size></Contents>
<Contents><Key>weekly logs/note</Key><Size>512</Size></Contents>
<Contents><Key>weekly logs/empty/</Key><Size>0</Size></Contents></ListBucketResult>`)
		case "two":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>
<Contents><Key>weekly logs/api/old/trace</Key><Size>1024</Size></Contents>
<Contents><Key>weekly logs/api</Key><Size>0</Size></Contents>
<Contents><Key>weekly logs/z/</Key><Size>512</Size></Contents>
<Contents><Key>weekly logs/worker/zero</Key><Size>0</Size></Contents></ListBucketResult>`)
		default:
			t.Errorf("unexpected continuation token: %s", r.URL)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()

	const root = "s3://bucket/weekly%20logs/"
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"sorted", nil, "SIZE\tOBJECTS\tPREFIX\n2.0 KiB\t2\t" + root + "api/\n512 B\t2\t" + root + " (direct objects)\n512 B\t1\t" + root + "z/\n0 B\t1\t" + root + "worker/\n3.0 KiB\t6\t" + root + " (total)\n"},
		{"top retains full total", []string{"--top", "1", "--bytes"}, "BYTES\tOBJECTS\tPREFIX\n2048\t2\t" + root + "api/\n3072\t6\t" + root + " (total)\n"},
		{"summary", []string{"-sb"}, "BYTES\tOBJECTS\tPREFIX\n3072\t6\t" + root + " (total)\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests.Store(0)
			got := cli(t, server.URL, "du", 0, append([]string{strings.TrimSuffix(root, "/")}, tc.args...)...)
			if got != tc.want || requests.Load() != 2 {
				t.Fatalf("%d requests, output:\n%s", requests.Load(), got)
			}
		})
	}
}

func TestDUEmptyBucketAndLargeExactTotal(t *testing.T) {
	for _, tc := range []struct{ name, contents, total string }{
		{"empty", "", "0\t0"},
		{"directory marker only", "<Contents><Key>empty/</Key><Size>0</Size></Contents>", "0\t0"},
		{"sum beyond int64", "<Contents><Key>a</Key><Size>9223372036854775807</Size></Contents><Contents><Key>b</Key><Size>9</Size></Contents>", "9223372036854775816\t2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("prefix") != "" || r.URL.Query().Get("list-type") != "2" {
					t.Errorf("unexpected request %s", r.URL)
				}
				fmt.Fprintf(w, "<ListBucketResult>%s</ListBucketResult>", tc.contents)
			}))
			defer server.Close()
			got := cli(t, server.URL, "du", 0, "s3://bucket", "-sb")
			if want := "BYTES\tOBJECTS\tPREFIX\n" + tc.total + "\ts3://bucket/ (total)\n"; got != want {
				t.Fatal(got)
			}
		})
	}
}

func TestDUNeverReportsPartialTotalsOnListFailure(t *testing.T) {
	for _, tc := range []struct {
		name, secondPage string
		status           int
	}{
		{"access denied", "<Error><Code>AccessDenied</Code></Error>", 403},
		{"missing size", "<ListBucketResult><Contents><Key>p/b</Key></Contents></ListBucketResult>", 200},
		{"negative size", "<ListBucketResult><Contents><Key>p/b</Key><Size>-1</Size></Contents></ListBucketResult>", 200},
		{"outside prefix", "<ListBucketResult><Contents><Key>p2/b</Key><Size>1</Size></Contents></ListBucketResult>", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("continuation-token") == "" {
					fmt.Fprint(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>b</NextContinuationToken><Contents><Key>p/a</Key><Size>1</Size></Contents></ListBucketResult>")
					return
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.secondPage)
			}))
			defer server.Close()
			var out, stderr bytes.Buffer
			code := Run("du", []string{"--endpoint", server.URL, "--region", "us-east-1", "--ak", "test", "--sk", "test", "s3://bucket/p/"}, &out, &stderr)
			if code != 1 || out.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, &out, &stderr)
			}
		})
	}
}
