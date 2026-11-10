package unzip

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestStandaloneValidation checks help/version and rejects invalid options without network access.
func TestStandaloneValidation(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"--version"}} {
		var out, errOut bytes.Buffer
		if code := Run(args, &out, &errOut); code != 0 || out.Len() == 0 || errOut.Len() != 0 {
			t.Fatalf("%v: %d %s", args, code, &errOut)
		}
	}
	for _, flags := range [][]string{{"--concurrency", "0"}, {"--download-concurrency", "0"}, {"--upload-concurrency", "129"}, {"--file-concurrency", "-1"}, {"--part-size", "1MiB"}} {
		args := append([]string{"s3://bucket/archive.zip", "s3://bucket/prefix"}, flags...)
		var out, errOut bytes.Buffer
		if code := Run(args, &out, &errOut); code != 2 || errOut.Len() == 0 {
			t.Fatalf("%v: %d %s", args, code, &errOut)
		}
	}
}

// TestSDKHTTPTransfer exercises real SDK serialization through an in-memory HTTP transport.
func TestSDKHTTPTransfer(t *testing.T) {
	archive := makeArchive(t, zip.Deflate, map[string][]byte{"empty": {}, "nested/hello.txt": []byte("hello")})
	var mu sync.Mutex
	objects := map[string][]byte{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/source/archive.zip":
			w.Header().Set("Content-Length", fmt.Sprint(len(archive)))
			w.Header().Set("ETag", "\"archive\"")
		case r.Method == http.MethodGet && r.URL.Path == "/source/archive.zip":
			var start, end int64
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || r.Header.Get("If-Match") != "\"archive\"" {
				http.Error(w, "bad range", 400)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(archive)))
			w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
			w.Header().Set("ETag", "\"archive\"")
			w.WriteHeader(206)
			_, _ = w.Write(archive[start : end+1])
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/dest/out/"):
			if r.Header.Get("If-None-Match") != "*" {
				http.Error(w, "missing condition", 400)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/empty") && (len(data) != 0 || r.ContentLength != 0) {
				http.Error(w, "bad empty object", 400)
				return
			}
			mu.Lock()
			objects[r.URL.Path] = data
			mu.Unlock()
			w.Header().Set("ETag", "\"uploaded\"")
		default:
			http.Error(w, "unexpected request", 400)
		}
	})
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		HTTPClient: &http.Client{Transport: handlerTransport{handler}},
	})
	_, opts := fixture(archive)
	if err := extract(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(objects) != 2 || string(objects["/dest/out/nested/hello.txt"]) != "hello" {
		t.Fatalf("objects=%v", objects)
	}
}

// handlerTransport runs real SDK HTTP requests without binding a listening socket.
type handlerTransport struct{ handler http.Handler }

// RoundTrip records a handler response, retaining the request metadata for the SDK.
func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body == nil {
		r.Body = http.NoBody
	}
	recorder := httptest.NewRecorder()
	t.handler.ServeHTTP(recorder, r)
	response := recorder.Result()
	response.Request = r
	return response, nil
}
