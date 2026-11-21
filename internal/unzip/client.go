package unzip

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/pkg"
)

// newTransferClient retains enough idle connections for configured request concurrency.
// This avoids repeated TCP/TLS setup between bursts of small-object uploads.
func newTransferClient(ctx context.Context, cfg pkg.Config, opts options) (*s3.Client, error) {
	client, err := pkg.NewS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	connections := opts.downloads + opts.uploads
	return pkg.WithS3IdleConnections(client, max(128, connections), max(32, connections)), nil
}
