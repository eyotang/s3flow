package update

import (
	"fmt"
	"io"
	"sync"
	"time"

	"charm.land/bubbles/v2/progress"
	"github.com/eyotang/s3flow/internal/pkg"
)

type progressReader struct {
	io.Reader
	done, total int64
	report      func(int64, int64)
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.done += int64(n)
	if n > 0 {
		r.report(r.done, r.total)
	}
	return n, err
}

type progressStatus struct {
	phase       string
	done, total int64
	started     time.Time
}

func (s progressStatus) view(bar progress.Model, now time.Time, frame int) string {
	activity := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}[frame%10]
	text := activity + " " + s.phase
	if s.phase != "下载更新包" {
		return text
	}
	seconds := max(now.Sub(s.started).Seconds(), 0.001)
	stats := fmt.Sprintf("%s · %s/s", pkg.FormatBytes(s.done), pkg.FormatBytes(int64(float64(s.done)/seconds)))
	if s.total > 0 {
		fraction := min(1, float64(s.done)/float64(s.total))
		stats = fmt.Sprintf("%s / %s · %s/s", pkg.FormatBytes(s.done), pkg.FormatBytes(s.total), pkg.FormatBytes(int64(float64(s.done)/seconds)))
		return fmt.Sprintf("%s %s %s", text, bar.ViewAs(fraction), stats)
	}
	return text + " " + stats
}

// showProgress refreshes even while waiting for network data. All terminal
// writes belong to this goroutine; finish waits for its final cleanup.
func showProgress(output io.Writer) (report func(string, int64, int64), finish func()) {
	var mu sync.Mutex
	status := progressStatus{phase: "获取校验文件", started: time.Now()}
	changed := make(chan struct{}, 1)
	stop, stopped := make(chan struct{}), make(chan struct{})
	report = func(phase string, done, total int64) {
		mu.Lock()
		phaseChanged := status.phase != phase
		if phaseChanged {
			status.started = time.Now()
		}
		status.phase, status.done, status.total = phase, done, total
		mu.Unlock()
		if phaseChanged {
			select {
			case changed <- struct{}{}:
			default:
			}
		}
	}
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		bar := progress.New(progress.WithWidth(24))
		frame := 0
		render := func() {
			mu.Lock()
			snapshot := status
			mu.Unlock()
			fmt.Fprintf(output, "\r\x1b[2K%s", snapshot.view(bar, time.Now(), frame))
			frame++
		}
		render()
		for {
			select {
			case <-changed:
				render()
			case <-ticker.C:
				render()
			case <-stop:
				fmt.Fprint(output, "\r\x1b[2K")
				return
			}
		}
	}()
	return report, func() { close(stop); <-stopped }
}
