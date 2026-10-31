package get

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type directoryClient struct {
	*fakeRangeS3
	pages []*s3.ListObjectsV2Output
	calls int
}

func (c *directoryClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	result := c.pages[c.calls]
	c.calls++
	return result, nil
}
func directoryObject(key string) types.Object {
	return types.Object{Key: aws.String(key), Size: aws.Int64(3)}
}
func TestDownloadDirectoryPaginationAndProgress(t *testing.T) {
	client := &directoryClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"}), pages: []*s3.ListObjectsV2Output{
		{Contents: []types.Object{directoryObject("dir/a")}, IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next")},
		{Contents: []types.Object{directoryObject("dir/nested/中文%2F.txt")}},
	}}
	dest := t.TempDir()
	var last downloadProgress
	err := downloadDirectory(context.Background(), client, downloadRequest{bucket: "bucket", key: "dir/", destination: dest, partSize: 4, concurrency: 2, recursive: true}, func(p downloadProgress) { last = p })
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "nested/中文%2F.txt"} {
		data, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil || string(data) != "abc" {
			t.Fatalf("%s: %q %v", name, data, err)
		}
	}
	if client.calls != 2 || last.total != 6 || last.downloaded != 6 || last.completedFiles != 2 {
		t.Fatalf("progress %+v", last)
	}
}
func TestDirectoryRejectsUnsafePathsAndConflicts(t *testing.T) {
	for _, keys := range [][]string{{"dir/../escape"}, {"dir/a/../../escape"}, {"dir//absolute"}, {"dir/a\\b"}, {"dir/a", "dir/a/b"}, {"outside/a"}} {
		objects := make([]types.Object, 0, len(keys))
		for _, key := range keys {
			objects = append(objects, directoryObject(key))
		}
		client := &directoryClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"}), pages: []*s3.ListObjectsV2Output{{Contents: objects}}}
		err := downloadDirectory(context.Background(), client, downloadRequest{key: "dir/", destination: t.TempDir(), partSize: 4, concurrency: 1}, func(downloadProgress) {})
		if err == nil || len(client.ranges) != 0 {
			t.Fatalf("accepted %v: %v", keys, err)
		}
	}
}
func TestDirectoryDoesNotFollowEscapingSymlink(t *testing.T) {
	dest, outside := t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dest, "nested")); err != nil {
		t.Fatal(err)
	}
	client := &directoryClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"}), pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{directoryObject("dir/nested/file")}}}}
	err := downloadDirectory(context.Background(), client, downloadRequest{key: "dir/", destination: dest, partSize: 4, concurrency: 1, force: true}, func(downloadProgress) {})
	if err == nil {
		t.Fatal("followed escaping symlink")
	}
	if _, err := os.Stat(filepath.Join(outside, "file")); !os.IsNotExist(err) {
		t.Fatal("wrote outside destination")
	}
}
func TestDirectorySourceSelection(t *testing.T) {
	for _, args := range [][]string{{"s3://bucket/dir/", "./out"}, {"--recursive", "s3://bucket/dir", "./out"}, {"s3://bucket", "./out"}} {
		opts, err := parseCLI(args)
		if err != nil {
			t.Fatal(err)
		}
		config, err := resolveConfig(opts)
		if err != nil {
			t.Fatal(err)
		}
		if !config.recursive || config.destination != "./out" {
			t.Fatalf("%+v", config)
		}
	}
}

type deniedDirectoryClient struct{ *directoryClient }

func (c *deniedDirectoryClient) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, statusError(403)
}
func TestDirectoryHeadFailureRetainsFileAndTotals(t *testing.T) {
	client := &deniedDirectoryClient{&directoryClient{pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{directoryObject("dir/crash (39) (1).txt"), directoryObject("dir/other")}}}}}
	events := make(chan downloadProgress, 32)
	runDownload(context.Background(), client, downloadRequest{bucket: "bucket", key: "dir/", recursive: true, destination: t.TempDir(), partSize: 4, concurrency: 1}, events)
	var final downloadProgress
	for p := range events {
		final = p
	}
	var response interface{ HTTPStatusCode() int }
	if !final.done || !errors.As(final.err, &response) || response.HTTPStatusCode() != 403 {
		t.Fatalf("unexpected error: %v", final.err)
	}
	if final.currentFile != "crash (39) (1).txt" || final.totalFiles != 2 || final.total != 6 || final.completedFiles != 0 {
		t.Fatalf("lost failure context: %+v", final)
	}
}
