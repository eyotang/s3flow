package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
)

type uploadAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

type uploadFile struct{ local, key string }

// A directory uploads its contents beneath the destination prefix. Symlinks
// and special files are rejected before any remote writes occur.
func planUpload(local, destination string) (string, []uploadFile, error) {
	if !strings.HasPrefix(destination, "s3://") {
		return "", nil, errors.New("目标必须是 s3://bucket/key 或 s3://bucket/prefix/")
	}
	remainder := strings.TrimPrefix(destination, "s3://")
	if !strings.Contains(remainder, "/") {
		destination += "/"
	}
	root := strings.HasSuffix(destination, "/")
	parseTarget := destination
	if root {
		parseTarget += "_"
	}
	bucket, key, err := pkg.ParseS3URI(parseTarget)
	if err != nil {
		return "", nil, err
	}
	if root {
		key = strings.TrimSuffix(key, "_")
	}
	info, err := os.Lstat(local)
	if err != nil {
		return "", nil, err
	}
	if info.Mode().IsRegular() {
		if root {
			key += filepath.Base(local)
		}
		return bucket, []uploadFile{{local, key}}, nil
	}
	if !info.IsDir() {
		return "", nil, fmt.Errorf("仅支持普通文件或目录: %q", local)
	}
	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var files []uploadFile
	err = filepath.WalkDir(local, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		stat, err := entry.Info()
		if err != nil {
			return err
		}
		if !stat.Mode().IsRegular() {
			return fmt.Errorf("目录包含符号链接或特殊文件: %q", name)
		}
		relative, err := filepath.Rel(local, name)
		if err != nil {
			return err
		}
		files = append(files, uploadFile{name, prefix + filepath.ToSlash(relative)})
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	if len(files) == 0 {
		return "", nil, fmt.Errorf("本地目录为空或仅包含空子目录，没有可上传的文件: %q", local)
	}
	return bucket, files, nil
}

func uploadObject(ctx context.Context, client uploadAPI, bucket string, file uploadFile, partSize int64, concurrency int, force bool, report func(objectProgress)) (err error) {
	if partSize < 5<<20 || partSize > 5<<30 || concurrency < 1 || concurrency > pkg.MaxConcurrency {
		return errors.New("无效的上传分片大小或并发数")
	}
	handle, err := os.Open(file.local)
	if err != nil {
		return err
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("上传源不是普通文件")
	}
	tracker := &uploadTracker{report: report, progress: objectProgress{stage: "上传中", total: info.Size(), totalParts: 1}}
	tracker.emit(0, 0)
	var condition *string
	if !force {
		condition = aws.String("*")
	}
	if info.Size() <= partSize {
		tracker.startPart(1, info.Size())
		// Empty objects have no payload. Avoid passing a non-nil stream with
		// zero ContentLength through SDK middleware and HTTP transports.
		var body io.Reader
		if info.Size() > 0 {
			body = &uploadReader{reader: io.NewSectionReader(handle, 0, info.Size()), tracker: tracker, part: 1}
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(file.key), Body: body, ContentLength: aws.Int64(info.Size()), IfNoneMatch: condition})
		if err == nil {
			tracker.finishPart(1)
		}
		return err
	}
	parts, err := pkg.PlanParts(info.Size(), partSize)
	if err != nil {
		return err
	}
	if parts[0].Size() > 5<<30 {
		return errors.New("对象过大：无法在 10000 个分片内上传")
	}
	tracker.progress.totalParts = len(parts)
	tracker.emit(0, 0)
	created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(file.key)})
	if err != nil {
		return err
	}
	if created == nil || aws.ToString(created.UploadId) == "" {
		return errors.New("S3 未返回 multipart upload ID")
	}
	complete := false
	defer func() {
		if complete {
			return
		}
		tracker.stage("清理未完成上传")
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, abortErr := client.AbortMultipartUpload(cleanup, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(file.key), UploadId: created.UploadId})
		if abortErr != nil {
			err = errors.Join(err, fmt.Errorf("清理未完成上传 %s 失败: %w", *created.UploadId, abortErr))
		}
	}()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make([]types.CompletedPart, len(parts))
	var next atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for range min(concurrency, len(parts)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if workerCtx.Err() != nil {
					return
				}
				index := int(next.Add(1) - 1)
				if index >= len(parts) {
					return
				}
				part := parts[index]
				tracker.startPart(index+1, part.Size())
				result, e := client.UploadPart(workerCtx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(file.key), UploadId: created.UploadId, PartNumber: aws.Int32(int32(index + 1)), Body: &uploadReader{reader: io.NewSectionReader(handle, part.Start, part.Size()), tracker: tracker, part: index + 1}, ContentLength: aws.Int64(part.Size())})
				if e == nil && (result == nil || aws.ToString(result.ETag) == "") {
					e = errors.New("S3 分片响应缺少 ETag")
				}
				if e != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("上传分片 %d: %w", index+1, e)
					}
					mu.Unlock()
					cancel()
					return
				}
				tracker.finishPart(index + 1)
				completed[index] = types.CompletedPart{PartNumber: aws.Int32(int32(index + 1)), ETag: result.ETag}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tracker.stage("合并分片")
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(file.key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}, IfNoneMatch: condition})
	if err != nil {
		return err
	}
	complete = true
	return nil
}

// uploadReader retains seeking for SDK retries. Rewinds subtract previously
// counted bytes, so signing passes and retries cannot inflate the total.
type uploadReader struct {
	reader  *io.SectionReader
	tracker *uploadTracker
	offset  int64
	part    int
}

func (r *uploadReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	r.tracker.partBytes(r.part, int64(n))
	return n, err
}
func (r *uploadReader) Seek(offset int64, whence int) (int64, error) {
	position, err := r.reader.Seek(offset, whence)
	if err == nil {
		// Seeking to the end to determine length is not uploaded data.
		counted := min(r.offset, max(0, position))
		r.tracker.partBytes(r.part, counted-r.offset)
		r.offset = counted
	}
	return position, err
}

type partProgress struct {
	number             int
	transferred, total int64
	finishedAt         time.Time
}

type objectProgress struct {
	parts                      []partProgress
	stage                      string
	transferred, total         int64
	completedParts, totalParts int
}
type uploadTracker struct {
	parts    map[int]partProgress
	mu       sync.Mutex
	progress objectProgress
	report   func(objectProgress)
}

func (t *uploadTracker) emit(delta int64, completed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.transferred += delta
	t.progress.completedParts += completed
	if t.report != nil {
		t.publishLocked()
	}
}
func (t *uploadTracker) stage(stage string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.stage = stage
	if t.report != nil {
		t.publishLocked()
	}
}

// All snapshots own their slice; worker updates cannot mutate UI messages.
func (t *uploadTracker) publishLocked() {
	snapshot := t.progress
	snapshot.parts = make([]partProgress, 0, len(t.parts))
	for _, part := range t.parts {
		snapshot.parts = append(snapshot.parts, part)
	}
	sort.Slice(snapshot.parts, func(i, j int) bool { return snapshot.parts[i].number < snapshot.parts[j].number })
	t.report(snapshot)
}
func (t *uploadTracker) startPart(number int, total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.parts == nil {
		t.parts = make(map[int]partProgress)
	}
	// Retain only a small completion tail; finished rows do not grow with the file.
	for id, part := range t.parts {
		if !part.finishedAt.IsZero() && (time.Since(part.finishedAt) > time.Second || id < number-2) {
			delete(t.parts, id)
		}
	}
	t.parts[number] = partProgress{number: number, total: total}
	if t.report != nil {
		t.publishLocked()
	}
}
func (t *uploadTracker) partBytes(number int, delta int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.transferred += delta
	if part, ok := t.parts[number]; ok {
		part.transferred += delta
		t.parts[number] = part
	}
	if t.report != nil {
		t.publishLocked()
	}
}
func (t *uploadTracker) finishPart(number int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.completedParts++
	part := t.parts[number]
	part.finishedAt = time.Now()
	t.parts[number] = part
	if t.report != nil {
		t.publishLocked()
	}
}

type uploadRequest struct {
	prefix      string
	bucket      string
	files       []uploadFile
	partSize    int64
	concurrency int
	force       bool
}
type uploadProgress struct {
	fileTransferred, fileTotal      int64
	parts                           []partProgress
	stage, currentFile, destination string
	uploaded, total                 int64
	completedParts, totalParts      int
	completedFiles, totalFiles      int
	done                            bool
	err                             error
}

func runUpload(ctx context.Context, client uploadAPI, request uploadRequest, events chan uploadProgress) {
	defer close(events)
	latest := uploadProgress{stage: "准备上传", totalFiles: len(request.files)}
	// Keep the newest snapshot, including terminal state, without blocking workers.
	publish := func() {
		select {
		case events <- latest:
			return
		default:
		}
		select {
		case <-events:
		default:
		}
		select {
		case events <- latest:
		default:
		}
	}
	finish := func(err error) {
		latest.done, latest.err = true, err
		latest.stage = "上传完成"
		if err != nil {
			latest.stage = "上传失败"
		}
		if errors.Is(err, context.Canceled) {
			latest.stage = "已取消"
		}
		publish()
	}
	if !request.force {
		latest.stage = "检查目标目录"
		publish()
		lister, ok := client.(pkg.ObjectLister)
		if !ok {
			finish(errors.New("客户端不支持检查目标目录"))
			return
		}
		if err := checkEmptyPrefix(ctx, lister, request.bucket, request.prefix); err != nil {
			finish(err)
			return
		}
	}
	sizes := make([]int64, len(request.files))
	for i, file := range request.files {
		if err := ctx.Err(); err != nil {
			finish(err)
			return
		}
		info, err := os.Stat(file.local)
		if err != nil {
			finish(err)
			return
		}
		sizes[i] = info.Size()
		latest.total += info.Size()
	}
	publish()
	var committed int64
	for i, file := range request.files {
		if err := ctx.Err(); err != nil {
			finish(err)
			return
		}
		latest.currentFile = file.local
		latest.destination = pkg.FormatS3URIForDisplay(request.bucket, file.key)
		latest.completedParts, latest.totalParts = 0, 0
		latest.parts = nil
		latest.fileTransferred, latest.fileTotal = 0, sizes[i]
		latest.stage = "准备上传"
		publish()
		err := uploadObject(ctx, client, request.bucket, file, request.partSize, request.concurrency, request.force, func(p objectProgress) {
			latest.fileTransferred, latest.fileTotal = p.transferred, p.total
			latest.stage = p.stage
			latest.parts = p.parts
			latest.total += p.total - sizes[i]
			sizes[i] = p.total
			latest.uploaded = committed + p.transferred
			latest.completedParts, latest.totalParts = p.completedParts, p.totalParts
			publish()
		})
		if err != nil {
			finish(err)
			return
		}
		committed += sizes[i]
		latest.uploaded = committed
		latest.completedFiles = i + 1
		publish()
	}
	if err := ctx.Err(); err != nil {
		finish(err)
		return
	}
	finish(nil)
}

func checkEmptyPrefix(ctx context.Context, client pkg.ObjectLister, bucket, prefix string) error {
	return pkg.WalkObjects(ctx, client, bucket, prefix, func(object types.Object) error {
		if aws.ToString(object.Key) == prefix && aws.ToInt64(object.Size) == 0 {
			return nil
		}
		return fmt.Errorf("目标目录 %s 非空，使用 --force 强推", pkg.FormatS3URIForDisplay(bucket, prefix))
	})
}
