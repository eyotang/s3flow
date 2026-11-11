package unzip

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
	"golang.org/x/sync/errgroup"
)

type transferAPI interface {
	sourceAPI
	pkg.ObjectLister
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

type entry struct {
	file *zip.File
	key  string
}

// planEntries validates all central-directory names, methods, sizes and duplicate targets before writes.
// Local header offsets are resolved by file workers to avoid serial per-entry network requests.
func planEntries(archive *zip.Reader, opts options, size int64) ([]entry, error) {
	entries := make([]entry, 0, len(archive.File))
	seen := make(map[string]bool)
	for _, f := range archive.File {
		name := strings.TrimSuffix(f.Name, "/")
		if name == "" || strings.ContainsAny(name, "\\\x00:") || strings.HasPrefix(name, "/") || path.Clean(name) != name {
			return nil, fmt.Errorf("不安全的 ZIP 路径: %q", f.Name)
		}
		for _, component := range strings.Split(name, "/") {
			if component == ".." || component == "." {
				return nil, fmt.Errorf("不安全的 ZIP 路径: %q", f.Name)
			}
		}
		if f.FileInfo().IsDir() {
			continue
		}
		if !f.Mode().IsRegular() || f.Flags&1 != 0 || (f.Method != zip.Store && f.Method != zip.Deflate) {
			return nil, fmt.Errorf("不支持的 ZIP 条目（链接、特殊文件、加密或压缩方法）: %q", f.Name)
		}
		key := opts.prefix + name
		if seen[key] {
			return nil, fmt.Errorf("ZIP 包含重复目标: %q", key)
		}
		seen[key] = true
		if opts.sourceBucket == opts.bucket && key == opts.sourceKey {
			return nil, errors.New("解压目标会覆盖源 ZIP")
		}
		if f.CompressedSize64 > uint64(size) || f.UncompressedSize64 > math.MaxInt64 {
			return nil, fmt.Errorf("ZIP 条目大小无效: %q", f.Name)
		}
		parts, err := pkg.PlanParts(int64(f.UncompressedSize64), opts.partSize)
		if err != nil {
			return nil, err
		}
		if len(parts) > 0 && parts[0].Size() > 5<<30 {
			return nil, fmt.Errorf("ZIP 条目过大: %q", f.Name)
		}
		entries = append(entries, entry{file: f, key: key})
	}
	return entries, nil
}

// extract runs bounded file workers and shares global download/upload limits.
func extract(ctx context.Context, client transferAPI, opts options) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	remote, err := openArchive(ctx, client, opts)
	if err != nil {
		return err
	}
	archive, err := zip.NewReader(remote, remote.size)
	if err != nil {
		return fmt.Errorf("读取 ZIP 索引: %w", err)
	}
	entries, err := planEntries(archive, opts, remote.size)
	if err != nil {
		return err
	}
	if !opts.force {
		err := pkg.WalkObjects(ctx, client, opts.bucket, opts.prefix, func(o types.Object) error {
			if aws.ToString(o.Key) == opts.prefix && aws.ToInt64(o.Size) == 0 {
				return nil
			}
			return errors.New("目标前缀非空，使用 --force 允许覆盖")
		})
		if err != nil {
			return err
		}
	}
	if opts.progress != nil {
		opts.progress.total.Store(int64(len(entries)))
	}
	slots := make(chan struct{}, opts.uploads)
	group, workerCtx := errgroup.WithContext(ctx)
	jobs := make(chan entry)
	remote.ctx = workerCtx
	for i := 0; i < min(opts.files, len(entries)); i++ {
		group.Go(func() error {
			for item := range jobs {
				if err := extractEntry(workerCtx, client, remote, opts, item, slots); err != nil {
					return fmt.Errorf("解压 %q: %w", item.file.Name, err)
				}
				if opts.progress != nil {
					opts.progress.completed.Add(1)
				}
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(jobs)
		for _, item := range entries {
			select {
			case jobs <- item:
			case <-workerCtx.Done():
				return workerCtx.Err()
			}
		}
		return nil
	})
	return group.Wait()
}

// extractEntry decompresses one pinned range stream and checks length/CRC before committing.
func extractEntry(ctx context.Context, client transferAPI, remote *remoteArchive, opts options, item entry, slots chan struct{}) error {
	ctx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	offset, err := validateEntryOffset(item, remote.size)
	if err != nil {
		return err
	}
	stream := remote.stream(ctx, offset, int64(item.file.CompressedSize64), opts.partSize, opts.downloads)
	defer stream.Close()
	var decoded io.Reader = stream
	if item.file.Method == zip.Deflate {
		inflater := flate.NewReader(stream)
		defer inflater.Close()
		decoded = inflater
	}
	checksum := crc32.NewIEEE()
	reader := io.TeeReader(decoded, checksum)
	verify := func() error {
		var extra [1]byte
		n, err := io.ReadFull(reader, extra[:])
		if n != 0 {
			return errors.New("ZIP 解压大小超过索引声明")
		}
		if err != io.EOF {
			return fmt.Errorf("ZIP 解压未正常结束: %w", err)
		}
		if checksum.Sum32() != item.file.CRC32 {
			return zip.ErrChecksum
		}
		return ctx.Err()
	}
	size := int64(item.file.UncompressedSize64)
	condition := (*string)(nil)
	if !opts.force {
		condition = aws.String("*")
	}
	if size <= opts.partSize {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		defer func() { <-slots }()
		buffer := make([]byte, int(size))
		if _, err := io.ReadFull(reader, buffer); err != nil {
			return err
		}
		if err := verify(); err != nil {
			return err
		}
		var body io.Reader
		if len(buffer) > 0 {
			body = bytes.NewReader(buffer)
		}
		_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(opts.bucket), Key: aws.String(item.key), ContentLength: aws.Int64(size), Body: body, IfNoneMatch: condition})
		return err
	}
	created, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(opts.bucket), Key: aws.String(item.key)})
	if err != nil {
		return err
	}
	if created == nil || aws.ToString(created.UploadId) == "" {
		return errors.New("S3 multipart 响应缺少 upload ID")
	}
	parts, err := uploadParts(ctx, client, opts, item.key, created.UploadId, size, reader, slots, verify, cancelRead)
	if err == nil {
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(opts.bucket), Key: aws.String(item.key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, IfNoneMatch: condition})
	}
	if err == nil {
		return nil
	}
	cancelRead()
	cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, cleanupErr := client.AbortMultipartUpload(cleanup, &s3.AbortMultipartUploadInput{Bucket: aws.String(opts.bucket), Key: aws.String(item.key), UploadId: created.UploadId})
	if cleanupErr != nil {
		return errors.Join(err, fmt.Errorf("中止 multipart 上传失败 (upload ID %s): %w", *created.UploadId, cleanupErr))
	}
	return err
}

// uploadParts reads decompressed bytes once, retaining seekable buffers for parallel SDK retries.
// Workers are joined on every failure before the caller aborts the multipart upload.
func uploadParts(ctx context.Context, client transferAPI, opts options, key string, id *string, size int64, reader io.Reader, slots chan struct{}, verify func() error, cancelRead context.CancelFunc) ([]types.CompletedPart, error) {
	plan, err := pkg.PlanParts(size, opts.partSize)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, workerCtx := errgroup.WithContext(ctx)
	completed := make([]types.CompletedPart, len(plan))
	var readErr error
	for i, part := range plan {
		select {
		case slots <- struct{}{}:
		case <-workerCtx.Done():
			readErr = workerCtx.Err()
		}
		if readErr != nil {
			break
		}
		if err := workerCtx.Err(); err != nil {
			<-slots
			readErr = err
			break
		}
		buffer := make([]byte, int(part.Size()))
		if _, err := io.ReadFull(reader, buffer); err != nil {
			<-slots
			readErr = err
			break
		}
		group.Go(func() error {
			defer func() { <-slots }()
			out, err := client.UploadPart(workerCtx, &s3.UploadPartInput{Bucket: aws.String(opts.bucket), Key: aws.String(key), UploadId: id, PartNumber: aws.Int32(int32(i + 1)), ContentLength: aws.Int64(part.Size()), Body: bytes.NewReader(buffer)})
			if err != nil {
				cancelRead()
				return err
			}
			if out == nil || aws.ToString(out.ETag) == "" {
				cancelRead()
				return errors.New("S3 分片响应缺少 ETag")
			}
			completed[i] = types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: out.ETag}
			return nil
		})
	}
	if readErr == nil {
		readErr = verify()
	}
	if readErr != nil {
		cancel()
	}
	uploadErr := group.Wait()
	if uploadErr != nil && !errors.Is(uploadErr, context.Canceled) {
		return nil, uploadErr
	}
	if readErr != nil {
		return nil, readErr
	}
	if uploadErr != nil {
		return nil, uploadErr
	}
	return completed, ctx.Err()
}
