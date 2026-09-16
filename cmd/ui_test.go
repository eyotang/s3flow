package main

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
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
		t.Fatal("expected progress synchronization and next-event commands")
	}
	if result.pendingPercent != 0.5 {
		t.Fatalf("expected pending progress target 0.5, got %v", result.pendingPercent)
	}
	if result.progress.Percent() != 0 {
		t.Fatalf("expected animated target to wait for synchronization, got %v", result.progress.Percent())
	}
	updated, animation := result.Update(syncAnimatedProgressMsg{})
	result = updated.(downloadModel)
	if animation == nil || result.progress.Percent() != 0.5 {
		t.Fatalf("expected synchronized animated target 0.5, got %v", result.progress.Percent())
	}
	updated, _ = result.Update(animation())
	result = updated.(downloadModel)
	if strings.HasSuffix(result.progress.View(), "  0%") {
		t.Fatalf("expected a rendered animation frame above 0%%, got:\n%s", result.progress.View())
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

func TestRapidProgressUpdatesDoNotStarveAnimatedProgress(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{concurrency: 8})
	defer model.cancel()

	updated, command := model.Update(downloadProgress{
		stage:      "下载中",
		downloaded: 1,
		total:      100,
	})
	result := updated.(downloadModel)
	if command == nil {
		t.Fatal("expected animation scheduling command")
	}
	batchMessage := command()
	batch, ok := batchMessage.(tea.BatchMsg)
	if !ok || len(batch) < 1 {
		t.Fatalf("expected a batch containing the progress command, got %T", batchMessage)
	}
	firstScheduledCommand := batch[0]

	for downloaded := int64(2); downloaded <= 50; downloaded++ {
		updated, _ = result.Update(downloadProgress{
			stage:      "下载中",
			downloaded: downloaded,
			total:      100,
		})
		result = updated.(downloadModel)
	}

	message := firstScheduledCommand()
	for range 3 {
		var next tea.Cmd
		updated, next = result.Update(message)
		result = updated.(downloadModel)
		if next == nil {
			break
		}
		message = next()
	}
	if strings.HasSuffix(result.progress.View(), "  0%") {
		t.Fatalf("rapid target updates starved animated frames:\n%s", result.progress.View())
	}
}
