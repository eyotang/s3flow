package get

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
)

const maxPartAttempts = 3

type downloadSource struct {
	bucket       string
	key          string
	presignedURL string
	display      string
	filename     string
}

type s3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type downloadRequest struct {
	head          *s3.HeadObjectOutput
	recursive     bool
	root          *os.Root
	bucket        string
	key           string
	sourceDisplay string
	destination   string
	partSize      int64
	concurrency   int
	force         bool
}

type downloadProgress struct {
	fileTransferred, fileTotal int64
	recursive                  bool
	sourceDisplay              string
	currentFile                string
	completedFiles, totalFiles int
	parts                      []partProgress
	stage                      string
	downloaded                 int64
	total                      int64
	completedParts             int
	totalParts                 int
	done                       bool
	err                        error
}

type progressFunc func(downloadProgress)

type presignedURLClient struct {
	rawURL  string
	display string
	client  *http.Client
}

func parseDownloadSource(raw string) (downloadSource, error) {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "s3://") {
		bucket, key, err := pkg.ParseS3URI(value)
		if err != nil {
			return downloadSource{}, err
		}
		return downloadSource{
			bucket:   bucket,
			key:      key,
			display:  pkg.FormatS3URIForDisplay(bucket, key),
			filename: path.Base(key),
		}, nil
	}

	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return downloadSource{}, errors.New("对象地址必须使用 s3://bucket/key 或 http(s) 预签名 URL 格式")
	}
	if parsed.User != nil {
		return downloadSource{}, errors.New("预签名 URL 不能包含 userinfo")
	}
	if parsed.Fragment != "" {
		return downloadSource{}, errors.New("预签名 URL 不能包含 fragment")
	}
	if err := validatePresignedSignedHeaders(parsed); err != nil {
		return downloadSource{}, err
	}
	filename := ""
	if escapedPath := parsed.EscapedPath(); escapedPath != "" && !strings.HasSuffix(escapedPath, "/") {
		filename = path.Base(parsed.Path)
	}
	return downloadSource{
		presignedURL: value,
		display:      safeURLDisplay(parsed),
		filename:     filename,
	}, nil
}

func safeURLDisplay(parsed *url.URL) string {
	copy := *parsed
	copy.RawQuery = ""
	copy.ForceQuery = false
	copy.Fragment = ""
	copy.User = nil
	return copy.String()
}

func validatePresignedSignedHeaders(parsed *url.URL) error {
	for name, values := range parsed.Query() {
		if !strings.EqualFold(name, "X-Amz-SignedHeaders") && !strings.EqualFold(name, "X-Goog-SignedHeaders") {
			continue
		}
		for _, value := range values {
			for _, header := range strings.Split(value, ";") {
				header = strings.ToLower(strings.TrimSpace(header))
				switch header {
				case "", "host":
					continue
				case "range":
					return errors.New("预签名 URL 已将 Range 请求头加入签名，无法为不同分片改写 Range；请生成仅签名 host 的 GET URL")
				default:
					return fmt.Errorf("预签名 URL 依赖未随 URL 提供的已签名请求头 %q", header)
				}
			}
		}
	}
	return nil
}

func newDownloadClient(ctx context.Context, cfg resolvedConfig) (s3API, error) {
	if cfg.presignedURL != "" {
		return newPresignedURLClient(cfg.presignedURL)
	}
	return pkg.NewS3Client(ctx, cfg.Config)
}

func newPresignedURLClient(rawURL string) (*presignedURLClient, error) {
	source, err := parseDownloadSource(rawURL)
	if err != nil {
		return nil, err
	}
	if source.presignedURL == "" {
		return nil, errors.New("预签名下载地址必须是 http(s) URL")
	}
	if _, err := http.NewRequest(http.MethodGet, source.presignedURL, nil); err != nil {
		return nil, errors.New("创建预签名 URL 请求失败：URL 格式无效")
	}

	transport := http.DefaultTransport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		clone := base.Clone()
		clone.MaxIdleConns = max(clone.MaxIdleConns, pkg.MaxConcurrency)
		clone.MaxIdleConnsPerHost = max(clone.MaxIdleConnsPerHost, pkg.MaxConcurrency)
		transport = clone
	}
	return &presignedURLClient{
		rawURL:  source.presignedURL,
		display: source.display,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *presignedURLClient) HeadObject(
	ctx context.Context,
	_ *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	response, err := c.doRangeRequest(ctx, "bytes=0-0", "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusPartialContent:
		start, end, total, err := parseContentRange(response.Header.Get("Content-Range"))
		if err != nil || start != 0 || end != 0 || total < 1 {
			return nil, fmt.Errorf("预签名 URL 探测返回无效的 Content-Range %q", response.Header.Get("Content-Range"))
		}
		read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 2))
		if readErr != nil {
			return nil, fmt.Errorf("读取预签名 URL 探测响应: %w", readErr)
		}
		if read != 1 {
			return nil, fmt.Errorf("预签名 URL 探测响应大小不匹配: 收到 %d，期望 1", read)
		}
		output := &s3.HeadObjectOutput{ContentLength: aws.Int64(total)}
		if etag := response.Header.Get("ETag"); etag != "" {
			output.ETag = aws.String(etag)
		}
		return output, nil

	case http.StatusRequestedRangeNotSatisfiable:
		total, ok := parseUnsatisfiedContentRange(response.Header.Get("Content-Range"))
		if ok && total == 0 {
			return &s3.HeadObjectOutput{ContentLength: aws.Int64(0)}, nil
		}
		return nil, c.statusError(response)

	case http.StatusOK:
		return nil, errors.New("服务端忽略了 Range 请求，无法安全地并行切片下载此 URL")

	default:
		return nil, c.statusError(response)
	}
}

func (c *presignedURLClient) GetObject(
	ctx context.Context,
	input *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	if input == nil || input.Range == nil || strings.TrimSpace(*input.Range) == "" {
		return nil, errors.New("预签名 URL 分片请求缺少 Range")
	}
	ifMatch := ""
	if input.IfMatch != nil {
		ifMatch = *input.IfMatch
	}
	response, err := c.doRangeRequest(ctx, *input.Range, ifMatch)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusPartialContent {
		defer response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return nil, errors.New("服务端忽略了 Range 请求，无法安全地并行切片下载此 URL")
		}
		return nil, c.statusError(response)
	}

	output := &s3.GetObjectOutput{
		Body:         response.Body,
		ContentRange: aws.String(response.Header.Get("Content-Range")),
	}
	if response.ContentLength >= 0 {
		output.ContentLength = aws.Int64(response.ContentLength)
	}
	if etag := response.Header.Get("ETag"); etag != "" {
		output.ETag = aws.String(etag)
	}
	return output, nil
}

func (c *presignedURLClient) doRangeRequest(ctx context.Context, rangeHeader, ifMatch string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.rawURL, nil)
	if err != nil {
		return nil, errors.New("创建预签名 URL 请求失败：URL 格式无效")
	}
	request.Header.Set("Range", rangeHeader)
	request.Header.Set("Accept-Encoding", "identity")
	if ifMatch != "" {
		request.Header.Set("If-Match", ifMatch)
	}
	response, err := c.client.Do(request)
	if err == nil {
		return response, nil
	}
	if contextError := ctx.Err(); contextError != nil {
		return nil, contextError
	}
	var urlError *url.Error
	if errors.As(err, &urlError) && urlError.Err != nil {
		err = urlError.Err
	}
	return nil, fmt.Errorf("请求预签名 URL %s: %w", c.display, err)
}

func (c *presignedURLClient) statusError(response *http.Response) error {
	requestID := response.Header.Get("x-amz-request-id")
	if requestID == "" {
		return fmt.Errorf("预签名 URL 请求失败: HTTP %s", response.Status)
	}
	return fmt.Errorf("预签名 URL 请求失败: HTTP %s（request id: %s）", response.Status, requestID)
}

func parseContentRange(raw string) (start, end, total int64, err error) {
	value := strings.TrimSpace(raw)
	rangeAndTotal, ok := strings.CutPrefix(value, "bytes ")
	if !ok {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	rangeValue, totalValue, ok := strings.Cut(rangeAndTotal, "/")
	if !ok || strings.Contains(totalValue, "/") {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	startValue, endValue, ok := strings.Cut(rangeValue, "-")
	if !ok || strings.Contains(endValue, "-") {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	start, startError := strconv.ParseInt(startValue, 10, 64)
	end, endError := strconv.ParseInt(endValue, 10, 64)
	total, totalError := strconv.ParseInt(totalValue, 10, 64)
	if startError != nil || endError != nil || totalError != nil || start < 0 || end < start || total <= end ||
		value != fmt.Sprintf("bytes %d-%d/%d", start, end, total) {
		return 0, 0, 0, fmt.Errorf("无效的 Content-Range %q", value)
	}
	return start, end, total, nil
}

func parseUnsatisfiedContentRange(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	totalValue, ok := strings.CutPrefix(value, "bytes */")
	if !ok {
		return 0, false
	}
	total, err := strconv.ParseInt(totalValue, 10, 64)
	return total, err == nil && total >= 0 && value == fmt.Sprintf("bytes */%d", total)
}

func runDownload(ctx context.Context, client s3API, request downloadRequest, events chan downloadProgress) {
	defer close(events)
	var latest downloadProgress
	var latestMu sync.Mutex
	report := func(progress downloadProgress) {
		progress.recursive = request.recursive
		progress.sourceDisplay = request.sourceDisplay
		latestMu.Lock()
		latest = progress
		latestMu.Unlock()
		select {
		case events <- progress:
		default:
		}
	}
	report(downloadProgress{stage: "读取对象信息"})
	resolved, err := detectDownloadSource(ctx, client, request)
	if err == nil {
		request = resolved
		if request.recursive {
			err = downloadDirectory(ctx, client, request, report)
		} else {
			err = downloadObject(ctx, client, request, report)
		}
	}
	latestMu.Lock()
	final := latest
	latestMu.Unlock()
	final.stage = "下载完成"
	final.done = true
	if err != nil {
		final.stage = "下载失败"
		final.err = err
		if errors.Is(err, context.Canceled) {
			final.stage = "已取消"
		}
	}
	// Replace one stale progress snapshot if necessary so a stopped UI cannot
	// leave the worker blocked while trying to publish its terminal state.
	select {
	case events <- final:
		return
	default:
	}
	select {
	case <-events:
	default:
	}
	select {
	case events <- final:
	default:
	}
}

func downloadObject(ctx context.Context, client s3API, request downloadRequest, report progressFunc) error {
	head := request.head
	var err error
	if head == nil {
		head, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(request.bucket), Key: aws.String(request.key)})
	}
	if err != nil {
		source := request.sourceDisplay
		if source == "" {
			source = fmt.Sprintf("s3://%s/%s", request.bucket, request.key)
		}
		return fmt.Errorf("读取 %s 元信息: %w", source, err)
	}
	if head == nil || head.ContentLength == nil || *head.ContentLength < 0 {
		return errors.New("S3 返回了无效的对象大小")
	}
	parts, err := pkg.PlanParts(*head.ContentLength, request.partSize)
	if err != nil {
		return err
	}
	total := *head.ContentLength
	etag := head.ETag
	stage := "下载中"
	if !isStrongETag(etag) {
		etag = nil
		if len(parts) > 1 {
			parts = []pkg.Part{{Index: 0, Start: 0, End: total - 1}}
			stage = "下载中（服务未提供强 ETag，已使用单分片）"
		}
	}
	report(downloadProgress{stage: stage, total: total, totalParts: len(parts)})
	if err := ctx.Err(); err != nil {
		return err
	}

	destination := request.destination
	lstat, mkdirAll, remove := os.Lstat, os.MkdirAll, os.Remove
	createTemp := os.CreateTemp
	commit := commitDownload
	if request.root != nil {
		root := request.root
		lstat, mkdirAll, remove = root.Lstat, root.MkdirAll, root.Remove
		createTemp = func(dir, pattern string) (*os.File, error) {
			return root.OpenFile(filepath.Join(dir, ".s3flow-"+rand.Text()+".part"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		}
		commit = func(ctx context.Context, temp, dest string, force bool) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if force {
				return root.Rename(temp, dest)
			}
			if err := root.Link(temp, dest); err != nil {
				return err
			}
			return root.Remove(temp)
		}
	} else {
		destination, err = filepath.Abs(destination)
		if err != nil {
			return err
		}
	}
	if err := prepareDestinationWith(destination, request.force, lstat); err != nil {
		return err
	}
	parent := filepath.Dir(destination)
	if err := mkdirAll(parent, 0755); err != nil {
		return err
	}
	temporary, err := createTemp(parent, "."+filepath.Base(destination)+"-*.part")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	if request.root != nil {
		temporaryName = filepath.Join(parent, filepath.Base(temporaryName))
	}
	committed := false
	defer func() {
		temporary.Close()
		if !committed {
			_ = remove(temporaryName)
		}
	}()
	if err := temporary.Truncate(total); err != nil {
		return fmt.Errorf("预分配临时文件: %w", err)
	}

	if len(parts) > 0 {
		if err := downloadParts(ctx, client, temporary, request, etag, parts, total, report); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("同步临时文件: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("关闭临时文件: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := commit(ctx, temporaryName, destination, request.force); err != nil {
		return err
	}
	committed = true
	report(downloadProgress{
		stage:          "校验并保存",
		downloaded:     total,
		total:          total,
		completedParts: len(parts),
		totalParts:     len(parts),
	})
	return nil
}

func isStrongETag(etag *string) bool {
	if etag == nil {
		return false
	}
	value := strings.TrimSpace(*etag)
	return value != "" && !strings.HasPrefix(strings.ToUpper(value), "W/")
}

func prepareDestination(destination string, force bool) error {
	return prepareDestinationWith(destination, force, os.Lstat)
}
func prepareDestinationWith(destination string, force bool, lstat func(string) (os.FileInfo, error)) error {
	info, err := lstat(destination)
	if err == nil {
		if info.IsDir() {
			return fmt.Errorf("输出路径是目录: %s", destination)
		}
		if !force {
			return fmt.Errorf("输出文件已存在: %s（使用 --force 覆盖）", destination)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("检查输出路径 %s: %w", destination, err)
	}
	return nil
}

func commitDownload(ctx context.Context, temporaryName, destination string, force bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if force {
		if err := os.Rename(temporaryName, destination); err != nil {
			return fmt.Errorf("保存下载文件: %w", err)
		}
		return nil
	}
	// A hard link gives us atomic no-clobber semantics because both files live
	// in the same directory. Rename would overwrite a file created mid-download.
	if err := os.Link(temporaryName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("输出文件已存在: %s（使用 --force 覆盖）", destination)
		}
		return fmt.Errorf("文件系统不支持安全提交: %w（确认目标路径可覆盖后可使用 --force）", err)
	}
	_ = os.Remove(temporaryName)
	return nil
}

func downloadParts(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	parts []pkg.Part,
	total int64,
	report progressFunc,
) error {
	workerCount := min(request.concurrency, len(parts))
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan pkg.Part)
	results := make(chan error, workerCount)
	var workers sync.WaitGroup
	var downloaded atomic.Int64
	var completed atomic.Int64
	var reportMu sync.Mutex
	states := map[int]partProgress{}
	emit := func(stage string, part pkg.Part, delta int64, done bool) {
		reportMu.Lock()
		defer reportMu.Unlock()
		for id, state := range states {
			if !state.finishedAt.IsZero() && (time.Since(state.finishedAt) > time.Second || id < part.Index-1) {
				delete(states, id)
			}
		}
		state := states[part.Index]
		state.number, state.total = part.Index+1, part.Size()
		state.transferred += delta
		if done {
			state.finishedAt = time.Now()
		}
		states[part.Index] = state
		rows := make([]partProgress, 0, len(states))
		for _, state := range states {
			rows = append(rows, state)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].number < rows[j].number })
		report(downloadProgress{stage: stage, downloaded: downloaded.Load(), total: total, completedParts: int(completed.Load()), totalParts: len(parts), parts: rows})
	}

	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for part := range jobs {
				emit("下载中", part, 0, false)
				err := downloadPartWithRetry(workerContext, client, file, request, etag, part, total, func(delta int64) {
					downloaded.Add(delta)
					emit("下载中", part, delta, false)
				})
				if err == nil {
					completed.Add(1)
					emit("下载中", part, 0, true)
				} else {
					cancel()
				}
				results <- err
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, part := range parts {
			select {
			case jobs <- part:
			case <-workerContext.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	var firstError error
	for result := range results {
		if result == nil {
			continue
		}
		if firstError == nil || (errors.Is(firstError, context.Canceled) && !errors.Is(result, context.Canceled)) {
			firstError = result
		}
	}
	if firstError != nil {
		return firstError
	}
	if downloaded.Load() != total {
		return fmt.Errorf("下载字节数不匹配: 收到 %d，期望 %d", downloaded.Load(), total)
	}
	return nil
}

func downloadPartWithRetry(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	part pkg.Part,
	total int64,
	onBytes func(int64),
) error {
	var lastError error
	for attempt := 1; attempt <= maxPartAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := downloadPart(ctx, client, file, request, etag, part, total, onBytes)
		if err == nil {
			return nil
		}
		if written > 0 {
			onBytes(-written)
		}
		lastError = err
		if attempt == maxPartAttempts {
			break
		}
		delay := time.Duration(1<<(attempt-1)) * 200 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return fmt.Errorf("分片 %d（bytes=%d-%d）下载失败: %w", part.Index+1, part.Start, part.End, lastError)
}

func downloadPart(
	ctx context.Context,
	client s3API,
	file *os.File,
	request downloadRequest,
	etag *string,
	part pkg.Part,
	total int64,
	onBytes func(int64),
) (int64, error) {
	byteRangeHeader := fmt.Sprintf("bytes=%d-%d", part.Start, part.End)
	input := &s3.GetObjectInput{
		Bucket: aws.String(request.bucket),
		Key:    aws.String(request.key),
		Range:  aws.String(byteRangeHeader),
	}
	if etag != nil && *etag != "" {
		input.IfMatch = etag
	}
	output, err := client.GetObject(ctx, input)
	if err != nil {
		return 0, err
	}
	if output.Body == nil {
		return 0, errors.New("S3 返回了空响应体")
	}
	if etag != nil && strings.TrimSpace(*etag) != "" {
		if output.ETag == nil || strings.TrimSpace(*output.ETag) != strings.TrimSpace(*etag) {
			_ = output.Body.Close()
			return 0, fmt.Errorf("分片响应 ETag 不匹配: 收到 %q，期望 %q", aws.ToString(output.ETag), strings.TrimSpace(*etag))
		}
	}
	expected := part.Size()
	if output.ContentLength != nil && *output.ContentLength != expected {
		_ = output.Body.Close()
		return 0, fmt.Errorf("分片响应大小不匹配: 收到 %d，期望 %d", *output.ContentLength, expected)
	}
	if err := validateContentRange(output.ContentRange, part, total); err != nil {
		_ = output.Body.Close()
		return 0, err
	}

	offsetWriter := io.NewOffsetWriter(file, part.Start)
	countingWriter := &countingWriter{writer: offsetWriter, onBytes: onBytes}
	written, copyError := io.CopyBuffer(countingWriter, io.LimitReader(output.Body, expected+1), make([]byte, 256<<10))
	closeError := output.Body.Close()
	if copyError != nil {
		return written, copyError
	}
	if closeError != nil {
		return written, closeError
	}
	if written != expected {
		return written, fmt.Errorf("分片响应大小不匹配: 收到 %d，期望 %d", written, expected)
	}
	return written, nil
}

func validateContentRange(raw *string, part pkg.Part, total int64) error {
	if raw == nil {
		return errors.New("分片响应缺少 Content-Range")
	}
	start, end, objectSize, err := parseContentRange(*raw)
	if err != nil || start != part.Start || end != part.End || objectSize != total {
		return fmt.Errorf("Content-Range 不匹配: 收到 %q，期望 %q", strings.TrimSpace(*raw),
			fmt.Sprintf("bytes %d-%d/%d", part.Start, part.End, total))
	}
	return nil
}

type countingWriter struct {
	writer  io.Writer
	onBytes func(int64)
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if n > 0 {
		w.onBytes(int64(n))
	}
	return n, err
}

// Map S3 keys literally; reject paths that cannot be represented safely rather
// than cleaning them into a different file name.
func relativeObjectPath(prefix, key string) (string, error) {
	if !strings.HasPrefix(key, prefix) {
		return "", errors.New("S3 列表返回了前缀外的对象")
	}
	relative := strings.TrimPrefix(key, prefix)
	if strings.ContainsAny(relative, "\\\x00:") || !filepath.IsLocal(relative) {
		return "", fmt.Errorf("不安全的对象路径 %q", key)
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." || component == ".." {
			return "", fmt.Errorf("不安全的对象路径 %q", key)
		}
	}
	return filepath.FromSlash(relative), nil
}

func downloadDirectory(ctx context.Context, client s3API, request downloadRequest, report progressFunc) error {
	lister, ok := client.(pkg.ObjectLister)
	if !ok {
		return errors.New("客户端不支持目录下载")
	}
	type entry struct {
		key, relative string
		size          int64
	}
	var files []entry
	destinations := map[string]bool{}
	var total int64
	report(downloadProgress{stage: "列举目录"})
	err := pkg.WalkObjects(ctx, lister, request.bucket, request.key, func(object types.Object) error {
		key := aws.ToString(object.Key)
		if strings.HasSuffix(key, "/") && aws.ToInt64(object.Size) == 0 {
			return nil
		}
		relative, err := relativeObjectPath(request.key, key)
		if err != nil {
			return err
		}
		if object.Size == nil || *object.Size < 0 {
			return errors.New("S3 返回无效的对象大小")
		}
		if destinations[relative] {
			return fmt.Errorf("重复的目标路径 %q", relative)
		}
		destinations[relative] = true
		files = append(files, entry{key, relative, *object.Size})
		total += *object.Size
		return nil
	})
	if err != nil {
		return err
	}
	for _, file := range files {
		for parent := filepath.Dir(file.relative); parent != "."; parent = filepath.Dir(parent) {
			if destinations[parent] {
				return fmt.Errorf("对象与目录路径冲突: %q", parent)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(request.destination, 0755); err != nil {
		return err
	}
	root, err := os.OpenRoot(request.destination)
	if err != nil {
		return err
	}
	defer root.Close()
	// Check known conflicts before writing the first object.
	for _, file := range files {
		if err := prepareDestinationWith(file.relative, request.force, root.Lstat); err != nil {
			return err
		}
	}
	report(downloadProgress{stage: "目录已列举", total: total, totalFiles: len(files)})
	var committed int64
	for i, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := request
		item.recursive = false
		item.key, item.destination, item.root = file.key, file.relative, root
		item.sourceDisplay = pkg.FormatS3URIForDisplay(item.bucket, item.key)
		size := file.size
		report(downloadProgress{stage: "读取对象信息", fileTotal: file.size, currentFile: file.relative, downloaded: committed, total: total, completedFiles: i, totalFiles: len(files)})
		err := downloadObject(ctx, client, item, func(p downloadProgress) {
			p.fileTransferred, p.fileTotal = p.downloaded, p.total
			total += p.total - size
			size = p.total
			p.total = total
			p.downloaded += committed
			p.currentFile = file.relative
			p.completedFiles = i
			p.totalFiles = len(files)
			report(p)
		})
		if err != nil {
			return err
		}
		committed += size
		report(downloadProgress{stage: "文件已保存", currentFile: file.relative, downloaded: committed, total: total, completedFiles: i + 1, totalFiles: len(files)})
	}
	report(downloadProgress{stage: "目录已保存", downloaded: committed, total: total, completedFiles: len(files), totalFiles: len(files)})
	return nil
}

type partProgress struct {
	number             int
	transferred, total int64
	finishedAt         time.Time
}

// Prefer an exact object. Only a missing object may fall back to key + "/";
// errors such as AccessDenied must never be reinterpreted as a directory.
func detectDownloadSource(ctx context.Context, client s3API, request downloadRequest) (downloadRequest, error) {
	lister, ok := client.(pkg.ObjectLister)
	if request.recursive || request.bucket == "" || !ok {
		return request, nil
	}
	if err := ctx.Err(); err != nil {
		return request, err
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(request.bucket), Key: aws.String(request.key)})
	if err == nil {
		request.head = head
		return request, nil
	}
	source := pkg.FormatS3URIForDisplay(request.bucket, request.key)
	headErr := fmt.Errorf("读取 %s 元信息: %w", source, err)
	if !isMissingObject(err) {
		return request, headErr
	}
	if err := ctx.Err(); err != nil {
		return request, err
	}
	prefix := request.key + "/"
	page, listErr := lister.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(request.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
	if listErr != nil {
		return request, fmt.Errorf("文件不存在，检查目录 %s 失败（需要 ListBucket 权限）: %w", pkg.FormatS3URIForDisplay(request.bucket, prefix), listErr)
	}
	if page == nil {
		return request, errors.New("检查目录时 S3 返回空的列表响应")
	}
	if len(page.Contents) == 0 {
		return request, fmt.Errorf("文件和同名目录均不存在: %w", headErr)
	}
	for _, object := range page.Contents {
		if !strings.HasPrefix(aws.ToString(object.Key), prefix) {
			return request, errors.New("检查目录时 S3 返回了前缀外的对象")
		}
	}
	request.key, request.recursive = prefix, true
	request.sourceDisplay = pkg.FormatS3URIForDisplay(request.bucket, prefix)
	return request, nil
}

func isMissingObject(err error) bool {
	var response interface{ HTTPStatusCode() int }
	if errors.As(err, &response) {
		return response.HTTPStatusCode() == http.StatusNotFound
	}
	var notFound *types.NotFound
	return errors.As(err, &notFound)
}
