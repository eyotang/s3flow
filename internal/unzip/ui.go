package unzip

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
)

const refreshInterval = 200 * time.Millisecond

type extractProgress struct {
	total     atomic.Int64
	completed atomic.Int64
}

type progressModel struct {
	counts *extractProgress
	done   <-chan struct{}
	bar    progress.Model
	closed bool
}

type refreshProgressMsg struct{}
type finishProgressMsg struct{}

// startProgress renders on a separate goroutine and returns a terminal cleanup function.
// Non-terminal output leaves the transfer's progress counter disabled.
func startProgress(out io.Writer) (*extractProgress, func()) {
	file, ok := out.(*os.File)
	if !ok || !term.IsTerminal(file.Fd()) {
		return nil, nil
	}

	counts := &extractProgress{}
	counts.total.Store(-1)
	done := make(chan struct{})
	stopped := make(chan struct{})
	model := progressModel{
		counts: counts,
		done:   done,
		bar: progress.New(
			progress.WithWidth(48),
			progress.WithColors(lipgloss.Color("#7D56F4")),
			progress.WithFillCharacters(progress.DefaultFullCharFullBlock, '█'),
			progress.WithoutPercentage(),
		),
	}
	program := tea.NewProgram(model, tea.WithOutput(out), tea.WithInput(nil),
		tea.WithoutSignalHandler(), tea.WithFPS(5))
	go func() {
		defer close(stopped)
		_, _ = program.Run()
	}()
	return counts, func() {
		close(done)
		<-stopped
	}
}

// Init schedules sampling and listens for transfer completion independently of the refresh interval.
func (m progressModel) Init() tea.Cmd {
	return tea.Batch(refreshProgress(), func() tea.Msg {
		<-m.done
		return finishProgressMsg{}
	})
}

// refreshProgress schedules one refresh without involving the transfer workers.
func refreshProgress() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg { return refreshProgressMsg{} })
}

// Update handles terminal width, refreshes and completion.
func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.bar.SetWidth(max(1, min(56, msg.Width-1)))
	case refreshProgressMsg:
		return m, refreshProgress()
	case finishProgressMsg:
		m.closed = true
		return m, tea.Quit
	}
	return m, nil
}

// View displays committed file counts and clears the transient UI when the transfer ends.
func (m progressModel) View() tea.View {
	if m.closed {
		return tea.NewView("")
	}
	total := m.counts.total.Load()
	if total < 0 {
		return tea.NewView("准备解压")
	}
	completed := m.counts.completed.Load()
	ratio := 1.0
	if total > 0 {
		ratio = float64(completed) / float64(total)
	}
	return tea.NewView(fmt.Sprintf("解压上传  %d/%d  %.0f%%\n%s",
		completed, total, ratio*100, m.bar.ViewAs(ratio)))
}
