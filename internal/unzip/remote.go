package unzip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

type sourceAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type remoteArchive struct {
	ctx           context.Context
	client        sourceAPI
	bucket, key   string
	size          int64
	etag, version *string
	slots         chan struct{}
	cache         *blockCache
}

// openArchive pins all metadata and payload reads to the HEAD version or strong ETag.
func openArchive(ctx context.Context, client sourceAPI, opts options) (*remoteArchive, error) {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(opts.sourceBucket), Key: aws.String(opts.sourceKey)})
	if err != nil {
		return nil, err
	}
	if head == nil || head.ContentLength == nil || *head.ContentLength < 0 {
		return nil, errors.New("S3 HEAD 缺少有效对象大小")
	}
	version := head.VersionId
	if aws.ToString(version) == "null" {
		version = nil
	}
	etag := head.ETag
	if v := aws.ToString(etag); v == "" || strings.HasPrefix(v, "W/") {
		etag = nil
	}
	if aws.ToString(version) == "" && etag == nil {
		return nil, errors.New("ZIP 并行读取需要对象版本号或强 ETag")
	}
	return &remoteArchive{ctx: ctx, client: client, bucket: opts.sourceBucket, key: opts.sourceKey, size: *head.ContentLength, etag: etag, version: version, slots: make(chan struct{}, opts.downloads), cache: &blockCache{blocks: make(map[int64]*cachedBlock), capacity: max(4, 2*opts.downloads), size: opts.partSize}}, nil
}

// ReadAt supplies ZIP central-directory random access without downloading the archive.
func (r *remoteArchive) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative ZIP offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.size-off)
	err := r.readCached(r.ctx, p[:int(n)], off)
	if err != nil {
		return 0, err
	}
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// readRange retries interrupted bodies and rejects ignored, changed or truncated ranges.
func (r *remoteArchive) readRange(ctx context.Context, p []byte, off int64) error {
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.slots }()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		err = r.fetchRange(ctx, p, off)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("下载 ZIP bytes=%d-%d: %w", off, off+int64(len(p))-1, err)
}

// fetchRange validates one exact response and closes its body on every path.
func (r *remoteArchive) fetchRange(ctx context.Context, p []byte, off int64) error {
	end := off + int64(len(p)) - 1
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(r.key), Range: aws.String(fmt.Sprintf("bytes=%d-%d", off, end)), IfMatch: r.etag, VersionId: r.version})
	if err != nil {
		return err
	}
	if out == nil || out.Body == nil {
		return errors.New("S3 Range 响应缺少 body")
	}
	defer out.Body.Close()
	expected := fmt.Sprintf("bytes %d-%d/%d", off, end, r.size)
	if aws.ToString(out.ContentRange) != expected || out.ContentLength == nil || *out.ContentLength != int64(len(p)) {
		return errors.New("S3 Range 响应范围或大小不匹配")
	}
	if r.etag != nil && aws.ToString(out.ETag) != *r.etag {
		return errors.New("ZIP 对象 ETag 已变化")
	}
	if aws.ToString(r.version) != "" && aws.ToString(out.VersionId) != *r.version {
		return errors.New("ZIP 对象版本已变化")
	}
	if _, err := io.ReadFull(out.Body, p); err != nil {
		return err
	}
	var extra [1]byte
	n, err := out.Body.Read(extra[:])
	if n != 0 || err != io.EOF {
		return errors.New("S3 Range 响应包含多余数据或未正常结束")
	}
	return nil
}

type rangeStream struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// Close cancels in-flight ranges and waits until all download buffers are released.
func (s *rangeStream) Close() error {
	s.once.Do(func() { s.cancel(); _ = s.PipeReader.Close(); <-s.done })
	return nil
}

// stream downloads bounded windows in parallel and exposes compressed bytes in order.
func (r *remoteArchive) stream(ctx context.Context, off, size, partSize int64, concurrency int) *rangeStream {
	ctx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	stream := &rangeStream{PipeReader: reader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(stream.done)
		err := r.writeRanges(ctx, writer, off, size, partSize, concurrency)
		_ = writer.CloseWithError(err)
	}()
	return stream
}

// writeRanges bounds buffered compressed data to concurrency times partSize per entry.
func (r *remoteArchive) writeRanges(ctx context.Context, w io.Writer, off, size, partSize int64, concurrency int) error {
	for remaining := size; remaining > 0; {
		count := min(int64(concurrency), (remaining-1)/partSize+1)
		buffers := make([][]byte, int(count))
		group, workerCtx := errgroup.WithContext(ctx)
		for i := range buffers {
			n := min(partSize-off%partSize, remaining)
			start := off
			buffers[i] = make([]byte, int(n))
			group.Go(func() error { return r.readCached(workerCtx, buffers[i], start) })
			off += n
			remaining -= n
		}
		if err := group.Wait(); err != nil {
			return err
		}
		for i, buffer := range buffers {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := w.Write(buffer); err != nil {
				return err
			}
			buffers[i] = nil
		}
	}
	return ctx.Err()
}
