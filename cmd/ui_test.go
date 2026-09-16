package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
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
	view := result.View().Content
	if !strings.Contains(view, "完成 · 文件已保存 · 耗时 ") || strings.Count(view, "耗时 ") != 1 {
		t.Fatalf("expected completed empty object to show elapsed time once in terminal state, got:\n%s", view)
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

func TestCompletedViewShowsElapsedInTerminalState(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{})
	defer model.cancel()
	model.startedAt = time.Now().Add(-3 * time.Second)

	updated, _ := model.Update(downloadProgress{
		stage:      "下载完成",
		downloaded: 100,
		total:      100,
		done:       true,
	})
	result := updated.(downloadModel)
	view := result.View().Content
	if !strings.Contains(view, "完成 · 文件已保存 · 耗时 ") {
		t.Fatalf("expected completed view to contain elapsed time in terminal state, got:\n%s", view)
	}
	if strings.Count(view, "耗时 ") != 1 {
		t.Fatalf("completed view must contain elapsed time exactly once, got:\n%s", view)
	}
	if strings.Contains(view, "剩余 ") {
		t.Fatalf("completed view must not contain remaining time, got:\n%s", view)
	}
	if result.finishedAt.IsZero() {
		t.Fatal("expected completion time to be frozen")
	}
	if result.elapsed() != result.finishedAt.Sub(result.startedAt) {
		t.Fatalf("elapsed time was not frozen at completion: %s", result.elapsed())
	}
}

func TestProgressUsesFullHeightContinuousCharacters(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{})
	defer model.cancel()
	if model.progress.Full != progress.DefaultFullCharFullBlock || model.progress.Empty != ' ' {
		t.Fatalf("progress characters = %q/%q, want full-height blocks with blank remainder", model.progress.Full, model.progress.Empty)
	}
	if model.progress.FullColor != lipgloss.Color(accentColor) {
		t.Fatalf("progress color = %v, want %s", model.progress.FullColor, accentColor)
	}
	rendered := model.progress.ViewAs(0.5)
	if !strings.ContainsRune(rendered, progress.DefaultFullCharFullBlock) {
		t.Fatalf("half-filled progress does not contain a full-height block: %q", rendered)
	}
	for _, unwanted := range []string{"━", "▌", "░"} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("half-filled progress contains segmented or thin character %q: %q", unwanted, rendered)
		}
	}
}

func TestActiveViewShowsRemainingInsteadOfElapsedTime(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{})
	defer model.cancel()
	model.startedAt = time.Now().Add(-10 * time.Second)

	updated, _ := model.Update(downloadProgress{
		stage:      "下载中",
		downloaded: 50,
		total:      100,
	})
	view := updated.(downloadModel).View().Content
	if !strings.Contains(view, "剩余 ") {
		t.Fatalf("expected active view to contain remaining time, got:\n%s", view)
	}
	if strings.Contains(view, "耗时 ") {
		t.Fatalf("active view must not contain final elapsed time, got:\n%s", view)
	}
}

func TestCanceledViewShowsTerminalStateAndElapsedTime(t *testing.T) {
	model := newDownloadModel(nil, downloadRequest{})
	defer model.cancel()
	model.startedAt = time.Now().Add(-2 * time.Second)
	model.cancelling = true

	updated, _ := model.Update(downloadProgress{
		stage: "已取消",
		done:  true,
		err:   context.Canceled,
	})
	view := updated.(downloadModel).View().Content
	if !strings.Contains(view, "已取消 · 耗时 ") {
		t.Fatalf("expected canceled view to contain terminal elapsed time, got:\n%s", view)
	}
	if strings.Count(view, "耗时 ") != 1 {
		t.Fatalf("canceled view must contain elapsed time exactly once, got:\n%s", view)
	}
	if strings.Contains(view, "正在取消") || strings.Contains(view, "剩余 ") {
		t.Fatalf("canceled terminal view contains an active-state label:\n%s", view)
	}
}

func TestFailedViewsShowElapsedTime(t *testing.T) {
	tests := []struct {
		name    string
		message tea.Msg
	}{
		{
			name: "download failure",
			message: downloadProgress{
				stage: "下载失败",
				done:  true,
				err:   errors.New("fixture failure"),
			},
		},
		{name: "event channel closed", message: downloadEventsClosedMsg{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := newDownloadModel(nil, downloadRequest{})
			defer model.cancel()
			model.startedAt = time.Now().Add(-time.Second)

			updated, _ := model.Update(test.message)
			result := updated.(downloadModel)
			view := result.View().Content
			for _, expected := range []string{"失败 ·", " · 耗时 "} {
				if !strings.Contains(view, expected) {
					t.Fatalf("expected failed view to contain %q, got:\n%s", expected, view)
				}
			}
			if strings.Count(view, "耗时 ") != 1 {
				t.Fatalf("failed view must contain elapsed time exactly once, got:\n%s", view)
			}
			if result.finishedAt.IsZero() || strings.Contains(view, "剩余 ") {
				t.Fatalf("failed terminal view did not freeze elapsed time:\n%s", view)
			}
		})
	}
}
