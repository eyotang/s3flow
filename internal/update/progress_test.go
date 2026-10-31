package update

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"charm.land/bubbles/v2/progress"
)

type chunkReader struct{ remaining string }

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.remaining == "" {
		return 0, io.EOF
	}
	n := copy(p, r.remaining[:min(2, len(r.remaining))])
	r.remaining = r.remaining[n:]
	return n, nil
}

func TestDownloadReportsProgressWhileReading(t *testing.T) {
	for _, total := range []int64{6, -1} {
		body := &chunkReader{remaining: "abcdef"}
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(body), ContentLength: total}, nil
		})}
		var updates []int64
		data, err := getWithProgress(context.Background(), client, api, "", "application/octet-stream", 10, func(done, size int64) {
			if size != total {
				t.Fatalf("total=%d, want %d", size, total)
			}
			if done == 2 && body.remaining == "" {
				t.Fatal("progress was delayed until download finished")
			}
			updates = append(updates, done)
		})
		if err != nil || string(data) != "abcdef" {
			t.Fatalf("data=%q err=%v", data, err)
		}
		if len(updates) != 4 || updates[0] != 0 || updates[1] != 2 || updates[3] != 6 {
			t.Fatalf("updates=%v", updates)
		}
	}
}

func TestProgressStatus(t *testing.T) {
	now := time.Now()
	s := progressStatus{phase: "下载更新包", done: 1024, total: 2048, started: now.Add(-time.Second)}
	view := s.view(progress.New(progress.WithWidth(12)), now, 0)
	for _, want := range []string{"下载更新包", "50%", "1.0 KiB / 2.0 KiB", "1.0 KiB/s"} {
		if !strings.Contains(view, want) {
			t.Fatalf("missing %q in %q", want, view)
		}
	}
	s.total = -1
	if view = s.view(progress.New(), now, 1); strings.Contains(view, "%") || !strings.Contains(view, "1.0 KiB/s") {
		t.Fatal(view)
	}
	for _, phase := range []string{"获取校验文件", "校验更新包", "解压更新包", "安装更新"} {
		s.phase = phase
		if view = s.view(progress.New(), now, 0); !strings.Contains(view, phase) || strings.Contains(view, "KiB") {
			t.Fatal(view)
		}
	}
}

type notifyingOutput struct {
	bytes.Buffer
	writes chan struct{}
}

func (w *notifyingOutput) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	select {
	case w.writes <- struct{}{}:
	default:
	}
	return n, err
}

func TestProgressRefreshesWhileWaitingAndCleansUp(t *testing.T) {
	output := &notifyingOutput{writes: make(chan struct{}, 10)}
	report, finish := showProgress(output)
	report("下载更新包", 1024, 2048)
	// Keep the network idle: the activity indicator must continue to refresh.
	for i := 0; i < 3; i++ {
		select {
		case <-output.writes:
		case <-time.After(2 * time.Second):
			finish()
			t.Fatal("progress stopped refreshing while waiting for data")
		}
	}
	finish()
	if !strings.Contains(output.String(), "下载更新包") || !strings.HasSuffix(output.String(), "\r\x1b[2K") {
		t.Fatal(output.String())
	}
}
