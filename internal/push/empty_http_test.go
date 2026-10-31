package push

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type uploadTransport func(*http.Request) (*http.Response, error)

func (f uploadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEmptyUploadHTTPRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	client := s3.New(s3.Options{
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported, Region: "us-east-1", BaseEndpoint: aws.String("https://s3.example.test"), UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		HTTPClient: &http.Client{Transport: uploadTransport(func(r *http.Request) (*http.Response, error) {
			if r.ContentLength != 0 || r.Body != nil || len(r.TransferEncoding) != 0 {
				t.Fatalf("empty request: length=%d body=%T encoding=%v", r.ContentLength, r.Body, r.TransferEncoding)
			}
			if r.Body != nil {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				if len(data) != 0 {
					t.Fatalf("unexpected payload: %q", data)
				}
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		})},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := uploadObject(ctx, client, "bucket", uploadFile{path, "empty"}, 5<<20, 1, false, nil); err != nil {
		t.Fatal(err)
	}
}
