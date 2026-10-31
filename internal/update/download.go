package update

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type responseError int

func (e responseError) Error() string { return fmt.Sprintf("GitHub HTTP %d", e) }

// Remove redirect URLs, which contain temporary signed download credentials.
// Preserve the underlying error so timeout and cancellation remain detectable.
func downloadError(err error) error {
	for {
		var u *url.Error
		if !errors.As(err, &u) {
			break
		}
		err = u.Err
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return fmt.Errorf("连接更新服务器超时：%w", err)
	}
	return err
}

func downloadAsset(ctx context.Context, client *http.Client, a asset, token string, limit int64, report func(string, int64, int64)) ([]byte, error) {
	phase := "下载更新包"
	if a.Name == "checksums.txt" {
		phase = "获取校验文件"
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		report(phase, 0, 0)
		data, err := getWithProgress(ctx, client, a.URL, token, "application/octet-stream", limit, func(done, total int64) {
			if a.Name != "checksums.txt" {
				report(phase, done, total)
			}
		})
		if err == nil {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var network net.Error
		var status responseError
		transient := (errors.As(err, &network) && (network.Timeout() || network.Temporary())) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || (errors.As(err, &status) && (status == 408 || status >= 500))
		if attempt >= 3 || !transient {
			return nil, fmt.Errorf("%s 下载失败：%w", a.Name, err)
		}
		report(fmt.Sprintf("%s失败，%d 秒后重试（%d/3）", phase, attempt, attempt+1), 0, 0)
		timer := time.NewTimer(time.Duration(attempt) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
