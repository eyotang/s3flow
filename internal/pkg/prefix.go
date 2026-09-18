package pkg

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type ObjectLister interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// ParseS3Location accepts bucket roots as well as object keys. Decodes once.
func ParseS3Location(uri string) (string, string, error) {
	if strings.HasPrefix(uri, "s3://") && !strings.Contains(strings.TrimPrefix(uri, "s3://"), "/") {
		uri += "/"
	}
	if strings.HasSuffix(uri, "/") {
		bucket, key, err := ParseS3URI(uri + "_")
		return bucket, strings.TrimSuffix(key, "_"), err
	}
	return ParseS3URI(uri)
}

func WalkObjects(ctx context.Context, client ObjectLister, bucket, prefix string, visit func(types.Object) error) error {
	var token *string
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			return err
		}
		if page == nil {
			return errors.New("S3 返回空的列表响应")
		}
		for _, object := range page.Contents {
			if err := visit(object); err != nil {
				return err
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return nil
		}
		next := aws.ToString(page.NextContinuationToken)
		if next == "" || seen[next] {
			return errors.New("S3 返回了无效的分页标记")
		}
		seen[next] = true
		token = page.NextContinuationToken
	}
}
