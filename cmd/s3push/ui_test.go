package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

func TestUploadProgressViewAndAnimation(t *testing.T) {
	model := newUploadModel(nil, uploadRequest{concurrency: 4})
	defer model.cancel()
	model.startedAt = time.Now().Add(-time.Second)
	updated, cmd := model.Update(uploadProgress{stage: "上传中", currentFile: "data/a.txt", destination: "s3://bucket/a.txt", uploaded: 5, total: 10, completedParts: 1, totalParts: 2, completedFiles: 1, totalFiles: 3})
	m := updated.(uploadModel)
	if cmd == nil || m.pendingPercent != 0.5 {
		t.Fatal("missing animated progress update")
	}
	updated, cmd = m.Update(syncAnimatedProgressMsg{})
	m = updated.(uploadModel)
	if cmd == nil || m.progress.Percent() != 0.5 {
		t.Fatal("animation did not receive percentage")
	}
	for _, want := range []string{"s3push", "data/a.txt", "s3://bucket/a.txt", "1/3", "1/2", "5 B / 10 B", "剩余"} {
		if !strings.Contains(m.View().Content, want) {
			t.Fatalf("missing %q: %s", want, m.View().Content)
		}
	}
}

func TestUploadCancelWaitsForWorker(t *testing.T) {
	model := newUploadModel(nil, uploadRequest{})
	defer model.cancel()
	updated, cmd := model.Update(tea.KeyPressMsg{Code: 'q', Text: "q"})
	m := updated.(uploadModel)
	if cmd != nil || m.done || !m.cancelling || model.ctx.Err() != context.Canceled {
		t.Fatal("cancel must wait for worker cleanup")
	}
	updated, _ = m.Update(uploadProgress{done: true, err: context.Canceled})
	m = updated.(uploadModel)
	if !m.done || !strings.Contains(m.View().Content, "已取消") {
		t.Fatal(m.View().Content)
	}
}

func TestUploadTerminalStates(t *testing.T) {
	for _, err := range []error{nil, errors.New("合并失败"), context.Canceled} {
		model := newUploadModel(nil, uploadRequest{})
		updated, _ := model.Update(uploadProgress{done: true, err: err})
		m := updated.(uploadModel)
		if !m.done || m.finishedAt.IsZero() {
			t.Fatal("missing terminal state")
		}
		elapsed := m.elapsed()
		if m.elapsed() != elapsed {
			t.Fatal("elapsed time is not frozen")
		}
		if err == nil && m.progress.Percent() != 1 {
			t.Fatal("empty upload did not complete")
		}
		if !strings.Contains(m.View().Content, "耗时") {
			t.Fatal(m.View().Content)
		}
		model.cancel()
	}
}

func TestProgressTrackVisibleAndCompact(t *testing.T) {
	model := newUploadModel(nil, uploadRequest{concurrency: 16})
	defer model.cancel()
	var parts []partProgress
	for i := 1; i <= 16; i++ {
		parts = append(parts, partProgress{number: i, total: 128 << 20})
	}
	updated, _ := model.Update(uploadProgress{currentFile: "large.bin", total: 3 << 40, fileTotal: 128 << 20, fileTransferred: 64 << 20, parts: parts})
	view := updated.(uploadModel).View().Content
	for _, want := range []string{"█", "整体进度", "当前文件", "50%"} {
		if !strings.Contains(view, want) {
			t.Fatalf("missing %q: %s", want, view)
		}
	}
	if strings.Contains(view, "分片 1 ") || strings.Count(view, "\n") > 23 {
		t.Fatalf("view flooded by part details: %s", view)
	}
}

func TestEmptyFilesProgressUsesCompletedFiles(t *testing.T) {
	m := newUploadModel(nil, uploadRequest{})
	defer m.cancel()
	m.totalFiles = 3
	m.completedFiles = 1
	if got := m.percent(); got != 1.0/3 {
		t.Fatalf("percent=%v", got)
	}
	m.completedFiles = 3
	if got := m.percent(); got != 1 {
		t.Fatalf("percent=%v", got)
	}
}

func TestEmptyFileProgressWaitsForCompletion(t *testing.T) {
	m := newUploadModel(nil, uploadRequest{})
	defer m.cancel()
	m.totalParts = 1
	if got := m.filePercent(); got != 0 {
		t.Fatalf("pending percent=%v", got)
	}
	m.completedParts = 1
	if got := m.filePercent(); got != 1 {
		t.Fatalf("completed percent=%v", got)
	}
}
