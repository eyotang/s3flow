package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const (
	accentColor          = "#7D56F4"
	progressSyncInterval = 100 * time.Millisecond
)

var (
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accentColor))
	labelStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#8A8A8A"))
	successStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#36C692"))
	errorStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FF5F87"))
	helpStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262"))
)

type downloadModel struct {
	client   s3API
	request  downloadRequest
	events   chan downloadProgress
	progress progress.Model

	ctx    context.Context
	cancel context.CancelFunc

	width                 int
	stage                 string
	downloaded            int64
	total                 int64
	completedParts        int
	totalParts            int
	startedAt             time.Time
	done                  bool
	cancelling            bool
	err                   error
	pendingPercent        float64
	progressSyncScheduled bool
}

type downloadEventsClosedMsg struct{}
type exitAfterCompletionMsg struct{}
type syncAnimatedProgressMsg struct{}

func newDownloadModel(client s3API, request downloadRequest) downloadModel {
	ctx, cancel := context.WithCancel(context.Background())
	bar := progress.New(
		progress.WithWidth(56),
		progress.WithColors(lipgloss.Color(accentColor)),
		progress.WithSpringOptions(20, 0.85),
	)
	return downloadModel{
		client:    client,
		request:   request,
		events:    make(chan downloadProgress, 32),
		progress:  bar,
		ctx:       ctx,
		cancel:    cancel,
		width:     80,
		stage:     "准备下载",
		startedAt: time.Now(),
	}
}

func (m downloadModel) Init() tea.Cmd {
	return tea.Batch(
		startDownloadCmd(m.ctx, m.client, m.request, m.events),
		waitDownloadProgressCmd(m.events),
	)
}

func startDownloadCmd(ctx context.Context, client s3API, request downloadRequest, events chan downloadProgress) tea.Cmd {
	return func() tea.Msg {
		runDownload(ctx, client, request, events)
		return nil
	}
}

func waitDownloadProgressCmd(events <-chan downloadProgress) tea.Cmd {
	return func() tea.Msg {
		message, ok := <-events
		if !ok {
			return downloadEventsClosedMsg{}
		}
		return message
	}
}

// syncAnimatedProgressCmd coalesces high-frequency byte updates so Bubbles
// animation frames are not invalidated before they can be rendered.
func syncAnimatedProgressCmd() tea.Cmd {
	return tea.Tick(progressSyncInterval, func(time.Time) tea.Msg {
		return syncAnimatedProgressMsg{}
	})
}

func (m downloadModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		m.width = message.Width
		m.progress.SetWidth(max(20, min(72, message.Width-8)))
		return m, nil

	case tea.KeyPressMsg:
		switch message.String() {
		case "q", "esc", "ctrl+c":
			if m.done {
				return m, tea.Quit
			}
			if !m.cancelling {
				m.cancelling = true
				m.stage = "正在取消"
				m.cancel()
			}
			return m, nil
		}

	case downloadProgress:
		m.stage = message.stage
		m.total = message.total
		m.downloaded = message.downloaded
		m.totalParts = message.totalParts
		m.completedParts = message.completedParts
		percent := m.percent()
		if message.done && message.err == nil {
			percent = 1
		}
		m.pendingPercent = percent
		if message.done {
			m.progressSyncScheduled = false
			animation := m.progress.SetPercent(percent)
			m.done = true
			m.err = message.err
			m.cancel()
			if m.cancelling || errors.Is(message.err, context.Canceled) {
				return m, tea.Batch(animation, tea.Quit)
			}
			return m, tea.Batch(animation, tea.Tick(650*time.Millisecond, func(time.Time) tea.Msg {
				return exitAfterCompletionMsg{}
			}))
		}
		if !m.progressSyncScheduled {
			m.progressSyncScheduled = true
			return m, tea.Batch(syncAnimatedProgressCmd(), waitDownloadProgressCmd(m.events))
		}
		return m, waitDownloadProgressCmd(m.events)

	case syncAnimatedProgressMsg:
		m.progressSyncScheduled = false
		if m.done {
			return m, nil
		}
		return m, m.progress.SetPercent(m.pendingPercent)

	case progress.FrameMsg:
		var command tea.Cmd
		m.progress, command = m.progress.Update(message)
		return m, command

	case downloadEventsClosedMsg:
		if !m.done {
			m.done = true
			m.err = errors.New("下载任务意外结束")
			m.stage = "下载失败"
		}
		return m, tea.Quit

	case exitAfterCompletionMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m downloadModel) View() tea.View {
	var builder strings.Builder
	builder.WriteString(titleStyle.Render("s3get · 并发分片下载"))
	builder.WriteString("\n\n")
	source := m.request.sourceDisplay
	if source == "" {
		source = fmt.Sprintf("s3://%s/%s", m.request.bucket, m.request.key)
	}
	writeDetail(&builder, "对象", source)
	writeDetail(&builder, "输出", m.request.destination)
	writeDetail(&builder, "分片", fmt.Sprintf("%d/%d · 并发 %d", m.completedParts, m.totalParts, m.request.concurrency))
	builder.WriteString("\n")
	builder.WriteString(m.progress.View())
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("%s / %s", formatBytes(m.downloaded), formatBytes(m.total)))
	if speed := m.bytesPerSecond(); speed > 0 {
		builder.WriteString(fmt.Sprintf("  %s/s", formatBytes(speed)))
		if eta := m.eta(speed); eta > 0 && !m.done {
			builder.WriteString(fmt.Sprintf("  剩余 %s", formatDuration(eta)))
		}
	}
	builder.WriteString("\n\n")
	switch {
	case m.err != nil && !errors.Is(m.err, context.Canceled):
		builder.WriteString(errorStyle.Render("失败 · " + m.err.Error()))
	case m.done && m.err == nil:
		builder.WriteString(successStyle.Render("完成 · 文件已保存"))
	case m.cancelling:
		builder.WriteString(labelStyle.Render("正在取消并清理临时文件…"))
	default:
		builder.WriteString(labelStyle.Render(m.stage))
	}
	builder.WriteString("\n\n")
	if m.done {
		builder.WriteString(helpStyle.Render("即将退出 · q 立即退出"))
	} else {
		builder.WriteString(helpStyle.Render("q / esc / ctrl+c 取消"))
	}

	view := tea.NewView(builder.String())
	view.WindowTitle = "s3get"
	return view
}

func writeDetail(builder *strings.Builder, label, value string) {
	builder.WriteString(labelStyle.Render(fmt.Sprintf("%-4s", label)))
	builder.WriteString("  ")
	builder.WriteString(value)
	builder.WriteByte('\n')
}

func (m downloadModel) percent() float64 {
	if m.total <= 0 {
		return 0
	}
	value := float64(m.downloaded) / float64(m.total)
	return max(0, min(1, value))
}

func (m downloadModel) bytesPerSecond() int64 {
	elapsed := time.Since(m.startedAt).Seconds()
	if elapsed <= 0 || m.downloaded <= 0 {
		return 0
	}
	return int64(float64(m.downloaded) / elapsed)
}

func (m downloadModel) eta(speed int64) time.Duration {
	if speed <= 0 || m.total <= m.downloaded {
		return 0
	}
	return time.Duration(float64(m.total-m.downloaded) / float64(speed) * float64(time.Second))
}

func formatDuration(value time.Duration) string {
	value = value.Round(time.Second)
	if value < time.Minute {
		return value.String()
	}
	hours := int(value / time.Hour)
	minutes := int(value%time.Hour) / int(time.Minute)
	seconds := int(value%time.Minute) / int(time.Second)
	if hours > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, seconds)
	}
	return fmt.Sprintf("%dm%02ds", minutes, seconds)
}
