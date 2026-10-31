package update

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVersions(t *testing.T) {
	for _, tt := range []struct {
		next, current string
		want          bool
	}{
		{"v0.10.0", "v0.9.9", true}, {"v1.0.0", "0.1.3", true},
		{"v0.1.3", "v0.1.3", false}, {"v0.1.2", "v0.1.3", false},
		{"v1.0.0-rc.1", "v0.1.3", false}, {"v01.0.0", "v0.1.3", false}, {"v1.0.0", "dev", false},
	} {
		if newer(tt.next, tt.current) != tt.want {
			t.Errorf("newer(%q,%q)", tt.next, tt.current)
		}
	}
}

func TestVerifiedArchiveAndReplace(t *testing.T) {
	for _, isZip := range []bool{false, true} {
		var archive bytes.Buffer
		content := []byte("new executable")
		if isZip {
			w := zip.NewWriter(&archive)
			f, err := w.Create("release/s3get")
			if err != nil {
				t.Fatal(err)
			}
			f.Write(content)
			w.Close()
		} else {
			gz := gzip.NewWriter(&archive)
			w := tar.NewWriter(gz)
			w.WriteHeader(&tar.Header{Name: "release/s3get", Mode: 0755, Size: int64(len(content))})
			w.Write(content)
			w.Close()
			gz.Close()
		}
		sum := sha256.Sum256(archive.Bytes())
		sums := []byte(fmt.Sprintf("%x  ./release.tar.gz\n", sum))
		if err := verify(archive.Bytes(), sums, "release.tar.gz"); err != nil {
			t.Fatal(err)
		}
		if verify([]byte("tampered"), sums, "release.tar.gz") == nil {
			t.Fatal("accepted tampered archive")
		}
		got, err := extract(archive.Bytes(), "s3get", isZip)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Fatal("wrong executable")
		}
		if _, err := extract(archive.Bytes(), "missing", isZip); err == nil {
			t.Fatal("accepted missing executable")
		}
		destination := filepath.Join(t.TempDir(), "s3get")
		if err := os.WriteFile(destination, []byte("old"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := replace(destination, got); err != nil {
			t.Fatal(err)
		}
		installed, err := os.ReadFile(destination)
		if err != nil || !bytes.Equal(installed, content) {
			t.Fatalf("replacement: %s %v", installed, err)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCheckDeadlineAndResponseLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	if _, err := get(ctx, client, api, "", "application/json", 1024); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline not enforced: %v", err)
	}
	for _, status := range []int{200, 404} {
		client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "Bearer test-token" {
				t.Fatal("missing private repo authentication")
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("12345")), Header: make(http.Header)}, nil
		})
		if _, err := get(context.Background(), client, api, "test-token", "application/json", 4); err == nil {
			t.Fatal("accepted oversized or unsuccessful response")
		}
	}
}

func TestExplicitConsent(t *testing.T) {
	for _, answer := range []string{"y\n", "YES\n", " Y \n"} {
		if !confirm(strings.NewReader(answer), io.Discard) {
			t.Fatalf("rejected %q", answer)
		}
	}
	for _, answer := range []string{"\n", "n\n", "no\n", "", "y"} {
		if confirm(strings.NewReader(answer), io.Discard) {
			t.Fatalf("accepted %q", answer)
		}
	}
}
