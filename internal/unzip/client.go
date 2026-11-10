package unzip

import (
	"context"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
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
	settings := client.Options()
	transport, ok := settings.HTTPClient.(*awshttp.BuildableClient)
	if !ok {
		return client, nil
	}
	connections := opts.downloads + opts.uploads
	settings.HTTPClient = transport.WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConns = max(128, connections)
		t.MaxIdleConnsPerHost = max(32, connections)
	})
	return s3.New(settings), nil
}
