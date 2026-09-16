package main

import (
	"strings"
	"testing"
)

func TestDownloadProgressDrivesAnimatedProgress(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{
		bucket:      "assets",
		key:         "object.bin",
		destination: "object.bin",
		concurrency: 4,
	})
	defer model.cancel()

	updated, command := model.Update(downloadProgress{
		stage:          "下载中",
		downloaded:     5,
		total:          10,
		completedParts: 1,
		totalParts:     2,
	})
	result := updated.(downloadModel)
	if command == nil {
		t.Fatal("expected SetPercent animation and next-event commands")
	}
	if result.progress.Percent() != 0.5 {
		t.Fatalf("expected animated progress target 0.5, got %v", result.progress.Percent())
	}
	view := result.View().Content
	for _, expected := range []string{"s3://assets/object.bin", "1/2", "5 B / 10 B"} {
		if !strings.Contains(view, expected) {
			t.Fatalf("expected view to contain %q, got:\n%s", expected, view)
		}
	}
}

func TestCompletedEmptyObjectAnimatesToFull(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{})
	defer model.cancel()

	updated, command := model.Update(downloadProgress{stage: "下载完成", done: true})
	result := updated.(downloadModel)
	if command == nil {
		t.Fatal("expected completion animation and exit commands")
	}
	if result.progress.Percent() != 1 {
		t.Fatalf("expected completed empty object to target 100%%, got %v", result.progress.Percent())
	}
}
