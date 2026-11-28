package objectcmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/encoding/httpbinding"
	"github.com/eyotang/s3flow/internal/pkg"
	"golang.org/x/sync/errgroup"
)

func (t transfer) copyObject(ctx context.Context, from, to location, state fileState, condition pkg.WriteCondition) error {
	source := httpbinding.EscapePath(from.bucket+"/"+from.path, false)
	if state.size > 5<<30 {
		return t.multipartCopy(ctx, from, to, source, state, condition)
	}
	result, err := t.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: &to.bucket, Key: &to.path, CopySource: &source,
		CopySourceIfMatch: state.etag, IfMatch: condition.IfMatch, IfNoneMatch: condition.IfNoneMatch,
	})
	if err != nil {
		return err
	}
	if result.CopyObjectResult == nil || aws.ToString(result.CopyObjectResult.ETag) == "" {
		return errors.New("S3 did not confirm the copied object")
	}
	return nil
}

func (t transfer) multipartCopy(ctx context.Context, from, to location, source string, state fileState, condition pkg.WriteCondition) (err error) {
	if !strongETag(state.etag) {
		return errors.New("multipart copy requires a source ETag")
	}
	parts, err := pkg.PlanParts(state.size, t.partSize.Value)
	if err != nil {
		return err
	}
	if parts[0].Size() > 5<<30 {
		return errors.New("object exceeds maximum multipart size")
	}
	head, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &from.bucket, Key: &from.path, IfMatch: state.etag})
	if err != nil {
		return err
	}
	if aws.ToString(head.ETag) != *state.etag || aws.ToInt64(head.ContentLength) != state.size {
		return errors.New("source changed before multipart copy")
	}
	create := &s3.CreateMultipartUploadInput{
		Bucket: &to.bucket, Key: &to.path,
		Metadata: head.Metadata, ContentType: head.ContentType, ContentEncoding: head.ContentEncoding,
		ContentDisposition: head.ContentDisposition, ContentLanguage: head.ContentLanguage,
		CacheControl: head.CacheControl, Expires: head.Expires, WebsiteRedirectLocation: head.WebsiteRedirectLocation,
	}
	if head.VersionId != nil {
		source += "?versionId=" + url.QueryEscape(*head.VersionId)
	}
	if head.TagCount == nil || *head.TagCount > 0 {
		tags, err := t.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: &from.bucket, Key: &from.path, VersionId: head.VersionId})
		if err != nil {
			return err
		}
		values := url.Values{}
		for _, tag := range tags.TagSet {
			values.Set(aws.ToString(tag.Key), aws.ToString(tag.Value))
		}
		if len(values) > 0 {
			create.Tagging = aws.String(values.Encode())
		}
	}
	upload, err := t.client.CreateMultipartUpload(ctx, create)
	if err != nil {
		return err
	}
	if aws.ToString(upload.UploadId) == "" {
		return errors.New("S3 did not return a multipart upload ID")
	}
	defer func() {
		if err == nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, abortErr := t.client.AbortMultipartUpload(cleanup, &s3.AbortMultipartUploadInput{Bucket: &to.bucket, Key: &to.path, UploadId: upload.UploadId})
		if abortErr != nil {
			err = errors.Join(err, fmt.Errorf("abort upload %s: %w", *upload.UploadId, abortErr))
		}
	}()

	completed := make([]types.CompletedPart, len(parts))
	workers, workerCtx := errgroup.WithContext(ctx)
	workers.SetLimit(t.concurrency)
	for _, part := range parts {
		if workerCtx.Err() != nil {
			break
		}
		workers.Go(func() error {
			number := int32(part.Index + 1)
			result, err := t.client.UploadPartCopy(workerCtx, &s3.UploadPartCopyInput{
				Bucket: &to.bucket, Key: &to.path, UploadId: upload.UploadId, PartNumber: &number,
				CopySource: &source, CopySourceIfMatch: state.etag, CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", part.Start, part.End)),
			})
			if err != nil {
				return err
			}
			r := result.CopyPartResult
			if r == nil || aws.ToString(r.ETag) == "" {
				return fmt.Errorf("missing ETag for part %d", number)
			}
			completed[part.Index] = types.CompletedPart{PartNumber: &number, ETag: r.ETag,
				ChecksumCRC32: r.ChecksumCRC32, ChecksumCRC32C: r.ChecksumCRC32C, ChecksumCRC64NVME: r.ChecksumCRC64NVME,
				ChecksumSHA1: r.ChecksumSHA1, ChecksumSHA256: r.ChecksumSHA256}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := t.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &to.bucket, Key: &to.path, UploadId: upload.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}, IfMatch: condition.IfMatch, IfNoneMatch: condition.IfNoneMatch,
	})
	if err != nil {
		return err
	}
	if aws.ToString(result.ETag) == "" {
		return errors.New("S3 did not confirm the completed multipart copy")
	}
	return nil
}
