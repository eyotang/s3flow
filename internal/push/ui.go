package push

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/eyotang/s3flow/internal/pkg"
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

type uploadModel struct {
	fileTransferred, fileTotal int64
	parts                      []partProgress
	height                     int
	client                     uploadAPI
	request                    uploadRequest
	events                     chan uploadProgress
	progress                   progress.Model

	ctx    context.Context
	cancel context.CancelFunc

	width                 int
	stage                 string
	uploaded              int64
	total                 int64
	currentFile           string
	destination           string
	completedFiles        int
	totalFiles            int
	completedParts        int
	totalParts            int
	startedAt             time.Time
	done                  bool
	cancelling            bool
	err                   error
	finishedAt            time.Time
	pendingPercent        float64
	progressSyncScheduled bool
}

type uploadEventsClosedMsg struct{}
type refreshTaskListMsg struct{}

func refreshTaskListCmd() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg { return refreshTaskListMsg{} })
}

type exitAfterCompletionMsg struct{}
type syncAnimatedProgressMsg struct{}

func newUploadModel(client uploadAPI, request uploadRequest) uploadModel {
	ctx, cancel := context.WithCancel(context.Background())
	bar := progress.New(
		progress.WithWidth(56),
		progress.WithColors(lipgloss.Color(accentColor)),
		progress.WithFillCharacters(progress.DefaultFullCharFullBlock, '█'),
		progress.WithSpringOptions(20, 0.85),
	)
	return uploadModel{
		client:    client,
		request:   request,
		events:    make(chan uploadProgress, 32),
		progress:  bar,
		ctx:       ctx,
		cancel:    cancel,
		width:     80,
		height:    30,
		stage:     "准备上传",
		startedAt: time.Now(),
	}
}

func (m uploadModel) Init() tea.Cmd {
	return tea.Batch(waitUploadProgressCmd(m.events), refreshTaskListCmd())
}

func waitUploadProgressCmd(events <-chan uploadProgress) tea.Cmd {
	return func() tea.Msg {
		message, ok := <-events
		if !ok {
			return uploadEventsClosedMsg{}
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

func (m uploadModel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case refreshTaskListMsg:
		if m.done {
			return m, nil
		}
		return m, refreshTaskListCmd()
	case tea.WindowSizeMsg:
		m.height = message.Height
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

	case uploadProgress:
		m.fileTransferred, m.fileTotal = message.fileTransferred, message.fileTotal
		m.parts = message.parts
		m.currentFile, m.destination = message.currentFile, message.destination
		m.completedFiles, m.totalFiles = message.completedFiles, message.totalFiles
		m.stage = message.stage
		m.total = message.total
		m.uploaded = message.uploaded
		m.totalParts = message.totalParts
		m.completedParts = message.completedParts
		percent := m.percent()
		if message.done && message.err == nil {
			percent = 1
		}
		m.pendingPercent = percent
		if message.done {
			if m.finishedAt.IsZero() {
				m.finishedAt = time.Now()
			}
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
			return m, tea.Batch(syncAnimatedProgressCmd(), waitUploadProgressCmd(m.events))
		}
		return m, waitUploadProgressCmd(m.events)

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

	case uploadEventsClosedMsg:
		if !m.done {
			m.done = true
			m.finishedAt = time.Now()
			m.err = errors.New("上传任务意外结束")
			m.stage = "上传失败"
		}
		return m, tea.Quit

	case exitAfterCompletionMsg:
		return m, tea.Quit
	}
	return m, nil
}

func (m uploadModel) View() tea.View {
	var builder strings.Builder
	builder.WriteString(titleStyle.Render("s3push · 并发分片上传"))
	builder.WriteString("\n\n")
	writeDetail(&builder, "文件", fmt.Sprintf("%q", m.currentFile))
	writeDetail(&builder, "目标", m.destination)
	writeDetail(&builder, "数量", fmt.Sprintf("%d/%d", m.completedFiles, m.totalFiles))
	writeDetail(&builder, "分片", fmt.Sprintf("%d/%d · 并发 %d", m.completedParts, m.totalParts, m.request.concurrency))
	builder.WriteString("\n")
	builder.WriteString(labelStyle.Render("整体进度") + "\n")
	builder.WriteString(m.progress.View())
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("%s / %s", pkg.FormatBytes(m.uploaded), pkg.FormatBytes(m.total)))
	speed := m.bytesPerSecond()
	if speed > 0 {
		builder.WriteString(fmt.Sprintf("  %s/s", pkg.FormatBytes(speed)))
	}
	if !m.done && speed > 0 {
		if eta := m.eta(speed); eta > 0 {
			builder.WriteString(fmt.Sprintf("  剩余 %s", formatDuration(eta)))
		}
	}
	builder.WriteString("\n\n")
	if m.currentFile != "" && !m.done {
		percent := m.filePercent()
		builder.WriteString(labelStyle.Render("当前文件") + "\n")
		builder.WriteString(m.progress.ViewAs(max(0, min(1, percent))))
		fmt.Fprintf(&builder, "\n%s / %s\n\n", pkg.FormatBytes(m.fileTransferred), pkg.FormatBytes(m.fileTotal))
	}
	switch {
	case m.err != nil && !errors.Is(m.err, context.Canceled):
		builder.WriteString(errorStyle.Render(fmt.Sprintf("失败 · %s · 耗时 %s", m.err.Error(), formatDuration(m.elapsed()))))
	case m.done && m.err == nil:
		builder.WriteString(successStyle.Render(fmt.Sprintf("完成 · 文件已上传 · 耗时 %s", formatDuration(m.elapsed()))))
	case m.done && errors.Is(m.err, context.Canceled):
		builder.WriteString(labelStyle.Render(fmt.Sprintf("已取消 · 耗时 %s", formatDuration(m.elapsed()))))
	case m.cancelling:
		builder.WriteString(labelStyle.Render("正在取消并清理未完成上传…"))
	default:
		builder.WriteString(labelStyle.Render(m.stage))
	}
	builder.WriteString("\n\n")
	if m.done {
		builder.WriteString(helpStyle.Render("即将退出 · q 立即退出"))
	} else {
		builder.WriteString(helpStyle.Render("q / esc / ctrl+c 取消"))
	}

	lines := strings.Split(builder.String(), "\n")
	for i, line := range lines {
		lines[i] = ansi.Truncate(line, max(1, m.width-1), "…")
	}
	view := tea.NewView(strings.Join(lines, "\n"))
	view.WindowTitle = "s3push"
	return view
}

func writeDetail(builder *strings.Builder, label, value string) {
	builder.WriteString(labelStyle.Render(fmt.Sprintf("%-4s", label)))
	builder.WriteString("  ")
	builder.WriteString(value)
	builder.WriteByte('\n')
}

func (m uploadModel) percent() float64 {
	if m.total <= 0 {
		if m.totalFiles > 0 {
			return max(0, min(1, float64(m.completedFiles)/float64(m.totalFiles)))
		}
		return 0
	}
	value := float64(m.uploaded) / float64(m.total)
	return max(0, min(1, value))
}

func (m uploadModel) bytesPerSecond() int64 {
	elapsed := m.elapsed().Seconds()
	if elapsed <= 0 || m.uploaded <= 0 {
		return 0
	}
	return int64(float64(m.uploaded) / elapsed)
}

// elapsed returns the upload runtime, frozen when a terminal progress event
// arrives so the UI exit delay is not included.
func (m uploadModel) elapsed() time.Duration {
	end := time.Now()
	if !m.finishedAt.IsZero() {
		end = m.finishedAt
	}
	if m.startedAt.IsZero() || end.Before(m.startedAt) {
		return 0
	}
	return end.Sub(m.startedAt)
}

func (m uploadModel) eta(speed int64) time.Duration {
	if speed <= 0 || m.total <= m.uploaded {
		return 0
	}
	return time.Duration(float64(m.total-m.uploaded) / float64(speed) * float64(time.Second))
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

// Zero-byte files complete when S3 acknowledges the upload, even though
// their byte count never changes.
func (m uploadModel) filePercent() float64 {
	if m.fileTotal > 0 {
		return max(0, min(1, float64(m.fileTransferred)/float64(m.fileTotal)))
	}
	if m.totalParts > 0 && m.completedParts >= m.totalParts {
		return 1
	}
	return 0
}
