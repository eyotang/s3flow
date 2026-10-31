package get

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/pkg"
)

func TestPresignedURLHeadObjectProbesWithRangeAndPreservesRawQuery(t *testing.T) {
	wantQuery := "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host&X-Amz-Signature=a%2Fb%2Bc&partNumber=7"
	rawURL := "https://storage.example.test/objects/archive.bin?" + wantQuery
	type receivedRequest struct {
		method   string
		rangeHdr string
		rawQuery string
	}
	received := make(chan receivedRequest, 1)

	transport := presignedRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		received <- receivedRequest{
			method:   request.Method,
			rangeHdr: request.Header.Get("Range"),
			rawQuery: request.URL.RawQuery,
		}
		headers := make(http.Header)
		headers.Set("Content-Range", "bytes 0-0/10")
		headers.Set("ETag", `"presigned-etag"`)
		return presignedTestResponse(request, http.StatusPartialContent, headers, []byte("0")), nil
	})

	client, err := newPresignedURLClient(rawURL)
	if err != nil {
		t.Fatalf("create presigned URL client: %v", err)
	}
	client.client = &http.Client{Transport: transport}

	head, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{})
	if err != nil {
		t.Fatalf("probe presigned URL: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != 10 {
		t.Fatalf("probe returned size %v, want 10", head.ContentLength)
	}

	request := <-received
	if request.method != http.MethodGet {
		t.Fatalf("probe method = %q, want GET", request.method)
	}
	if request.rangeHdr != "bytes=0-0" {
		t.Fatalf("probe Range = %q, want bytes=0-0", request.rangeHdr)
	}
	if request.rawQuery != wantQuery {
		t.Fatalf("raw query changed: got %q, want %q", request.rawQuery, wantQuery)
	}
}

func TestPresignedURLDownloadUsesConcurrentRangesAndAssemblesFile(t *testing.T) {
	object := []byte("0123456789abcdefghij")
	wantQuery := "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host&X-Amz-Signature=keep%2Fthis%2Bexact"

	var currentRequests atomic.Int32
	var maximumRequests atomic.Int32
	var queryChanged atomic.Bool
	var invalidHeaders atomic.Bool
	var rangesMu sync.Mutex
	var ranges []string

	transport := presignedRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.RawQuery != wantQuery {
			queryChanged.Store(true)
			return presignedTestResponse(request, http.StatusForbidden, nil, []byte("query changed")), nil
		}

		rangeHeader := request.Header.Get("Range")
		if request.Header.Get("Accept-Encoding") != "identity" ||
			(rangeHeader != "bytes=0-0" && request.Header.Get("If-Match") != `"object-etag"`) {
			invalidHeaders.Store(true)
		}
		start, end, err := parsePresignedTestRange(rangeHeader, int64(len(object)))
		if err != nil {
			return presignedTestResponse(request, http.StatusRequestedRangeNotSatisfiable, nil, []byte(err.Error())), nil
		}
		rangesMu.Lock()
		ranges = append(ranges, rangeHeader)
		rangesMu.Unlock()

		// The 0-0 request is the metadata probe and runs before part workers.
		// Holding data requests briefly makes overlapping workers observable.
		if rangeHeader != "bytes=0-0" {
			active := currentRequests.Add(1)
			for {
				seen := maximumRequests.Load()
				if active <= seen || maximumRequests.CompareAndSwap(seen, active) {
					break
				}
			}
			defer currentRequests.Add(-1)
			time.Sleep(25 * time.Millisecond)
		}

		payload := object[start : end+1]
		headers := make(http.Header)
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(object)))
		headers.Set("ETag", `"object-etag"`)
		return presignedTestResponse(request, http.StatusPartialContent, headers, payload), nil
	})

	rawURL := "https://storage.example.test/objects/archive.bin?" + wantQuery
	client, err := newPresignedURLClient(rawURL)
	if err != nil {
		t.Fatalf("create presigned URL client: %v", err)
	}
	client.client = &http.Client{Transport: transport}

	destination := filepath.Join(t.TempDir(), "archive.bin")
	err = downloadObject(context.Background(), client, downloadRequest{
		sourceDisplay: client.display,
		destination:   destination,
		partSize:      4,
		concurrency:   4,
	}, func(downloadProgress) {})
	if err != nil {
		t.Fatalf("download presigned URL: %v", err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read assembled download: %v", err)
	}
	if !reflect.DeepEqual(got, object) {
		t.Fatalf("assembled content = %q, want %q", got, object)
	}
	if queryChanged.Load() {
		t.Fatal("at least one request changed the original raw query")
	}
	if invalidHeaders.Load() {
		t.Fatal("range requests did not preserve identity encoding and the probe ETag")
	}
	if maximumRequests.Load() < 2 {
		t.Fatalf("maximum concurrent part requests = %d, want at least 2", maximumRequests.Load())
	}

	rangesMu.Lock()
	gotRanges := append([]string(nil), ranges...)
	rangesMu.Unlock()
	sort.Strings(gotRanges)
	wantRanges := []string{
		"bytes=0-0", // metadata probe
		"bytes=0-3",
		"bytes=4-7",
		"bytes=8-11",
		"bytes=12-15",
		"bytes=16-19",
	}
	sort.Strings(wantRanges)
	if !reflect.DeepEqual(gotRanges, wantRanges) {
		t.Fatalf("Range requests = %v, want %v", gotRanges, wantRanges)
	}
}

func TestPresignedURLRejectsSignedRangeHeader(t *testing.T) {
	rawURL := "https://storage.example.test/object.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host%3Brange%3Bx-amz-date&X-Amz-Signature=signature"
	_, err := newPresignedURLClient(rawURL)
	if err == nil {
		t.Fatal("expected URL that signs the Range header to be rejected")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "range") {
		t.Fatalf("error %q does not explain that Range is signed", err)
	}
}

func TestResolveConfigPresignedURLDerivesSafeOutputName(t *testing.T) {
	clearPresignedTestAWSEnvironment(t)
	rawURL := "https://storage.example.test/releases/report%20final.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak%2Fdate%2Fregion%2Fs3%2Faws4_request&X-Amz-Signature=super-secret"

	resolved, err := resolveConfig(cliOptions{
		source:      rawURL,
		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	})
	if err != nil {
		t.Fatalf("resolve presigned URL without credentials: %v", err)
	}
	if resolved.presignedURL != rawURL {
		t.Fatalf("presigned URL changed: got %q, want %q", resolved.presignedURL, rawURL)
	}
	if resolved.destination != "report final.tar.gz" {
		t.Fatalf("derived output = %q, want %q", resolved.destination, "report final.tar.gz")
	}
	if strings.Contains(resolved.destination, "?") || strings.Contains(resolved.destination, "X-Amz-") {
		t.Fatalf("query parameters leaked into output filename %q", resolved.destination)
	}
	if strings.Contains(resolved.sourceDisplay, "?") || strings.Contains(resolved.sourceDisplay, "super-secret") {
		t.Fatalf("query parameters leaked into display source %q", resolved.sourceDisplay)
	}
}

func TestInvalidPresignedURLDoesNotLeakSignatureInError(t *testing.T) {
	const secret = "super-secret-signature"
	_, err := parseDownloadSource("https://storage.example.test/%zz?X-Amz-Signature=" + secret)
	if err == nil {
		t.Fatal("expected malformed URL to fail")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "X-Amz-Signature") {
		t.Fatalf("configuration error leaked signed query: %q", err)
	}
}

func TestPresignedURLRequestErrorDoesNotLeakSignature(t *testing.T) {
	const secret = "runtime-secret-signature"
	client, err := newPresignedURLClient("https://storage.example.test/object.bin?X-Amz-SignedHeaders=host&X-Amz-Signature=" + secret)
	if err != nil {
		t.Fatalf("create presigned client: %v", err)
	}
	client.client = &http.Client{Transport: presignedRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	})}

	_, err = client.HeadObject(context.Background(), &s3.HeadObjectInput{})
	if err == nil {
		t.Fatal("expected request to fail")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "X-Amz-Signature") {
		t.Fatalf("request error leaked signed query: %q", err)
	}
}

func parsePresignedTestRange(value string, size int64) (int64, int64, error) {
	var start, end int64
	if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
		return 0, 0, fmt.Errorf("invalid Range %q", value)
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, fmt.Errorf("out-of-bounds Range %q", value)
	}
	return start, end, nil
}

func clearPresignedTestAWSEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		t.Setenv(name, "")
	}
}

type presignedRoundTripperFunc func(*http.Request) (*http.Response, error)

func (roundTrip presignedRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func presignedTestResponse(
	request *http.Request,
	statusCode int,
	headers http.Header,
	payload []byte,
) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Length", fmt.Sprint(len(payload)))
	return &http.Response{
		StatusCode:    statusCode,
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Header:        headers,
		Body:          io.NopCloser(bytes.NewReader(payload)),
		ContentLength: int64(len(payload)),
		Request:       request,
	}
}
