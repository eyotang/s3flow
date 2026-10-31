package update

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type handshakeTimeout struct{}

func (handshakeTimeout) Error() string   { return "net/http: TLS handshake timeout" }
func (handshakeTimeout) Timeout() bool   { return true }
func (handshakeTimeout) Temporary() bool { return true }

func TestAssetDownloadRetriesTLSFailure(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, &url.Error{Op: "Get", URL: "https://release-assets.githubusercontent.com/asset?sig=secret", Err: handshakeTimeout{}}
		}
		return &http.Response{StatusCode: 200, ContentLength: 2, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	var phases []string
	data, err := downloadAsset(context.Background(), client, asset{Name: "checksums.txt", URL: api + "/asset"}, "", 1024, func(phase string, _, _ int64) { phases = append(phases, phase) })
	if err != nil || string(data) != "ok" || attempts != 2 {
		t.Fatalf("attempts=%d data=%q err=%v", attempts, data, err)
	}
	if !strings.Contains(strings.Join(phases, " "), "重试") {
		t.Fatal("missing retry status")
	}
}

func TestDownloadErrorsHideSignedURLs(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Get", URL: "https://release-assets.githubusercontent.com/asset?sig=secret", Err: handshakeTimeout{}}
	})}
	_, err := get(context.Background(), client, api, "", "application/octet-stream", 10)
	if err == nil || strings.Contains(err.Error(), "sig=") || strings.Contains(err.Error(), "https://") {
		t.Fatalf("unsanitized error: %v", err)
	}
}

func TestAssetRetryHonorsDeadlineAndPermanentErrors(t *testing.T) {
	for _, status := range []int{0, 403} {
		attempts := 0
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			if status == 0 {
				return nil, handshakeTimeout{}
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("forbidden"))}, nil
		})}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := downloadAsset(ctx, client, asset{Name: "checksums.txt", URL: api}, "", 10, func(string, int64, int64) {})
		cancel()
		if err == nil || attempts != 1 {
			t.Fatalf("attempts=%d err=%v", attempts, err)
		}
		if status == 0 && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline: %v", err)
		}
	}
}

func TestAssetRetriesStopAfterThreeAttempts(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})}
	_, err := downloadAsset(context.Background(), client, asset{Name: "checksums.txt", URL: api}, "", 10, func(string, int64, int64) {})
	if err == nil || attempts != 3 || !strings.Contains(err.Error(), "checksums.txt") {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}
