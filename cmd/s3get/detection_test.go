package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type detectionClient struct {
	*fakeRangeS3
	headErr, listErr error
	objects          []types.Object
	heads, lists     int
	lastList         *s3.ListObjectsV2Input
}

func (c *detectionClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.heads++
	if in.Key != nil && *in.Key == "dir" && c.headErr != nil {
		return nil, c.headErr
	}
	return c.fakeRangeS3.HeadObject(ctx, in, opts...)
}
func (c *detectionClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.lists++
	c.lastList = in
	if c.listErr != nil {
		return nil, c.listErr
	}
	return &s3.ListObjectsV2Output{Contents: c.objects}, nil
}
func statusError(code int) error {
	return fmt.Errorf("wrapped: %w", &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}}, Err: errors.New("S3 error")})
}
func TestDetectDownloadSource(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		headErr, listErr       error
		found, recursive, fail bool
		wantLists              int
	}{
		{name: "file wins", found: true},
		{name: "404 finds directory", headErr: statusError(404), found: true, recursive: true, wantLists: 1},
		{name: "modeled NotFound", headErr: &types.NotFound{}, found: true, recursive: true, wantLists: 1},
		{name: "missing both", headErr: statusError(404), fail: true, wantLists: 1},
		{name: "403 is not a directory", headErr: statusError(403), found: true, fail: true},
		{name: "network error", headErr: errors.New("connection failed"), found: true, fail: true},
		{name: "list denied", headErr: statusError(404), listErr: statusError(403), fail: true, wantLists: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &detectionClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"}), headErr: tc.headErr, listErr: tc.listErr}
			if tc.found {
				client.objects = []types.Object{directoryObject("dir/file")}
			}
			result, err := detectDownloadSource(context.Background(), client, downloadRequest{bucket: "bucket", key: "dir", destination: "local"})
			if (err != nil) != tc.fail || result.recursive != tc.recursive || client.lists != tc.wantLists {
				t.Fatalf("result=%+v err=%v lists=%d", result, err, client.lists)
			}
			if client.lastList != nil && (aws.ToString(client.lastList.Prefix) != "dir/" || aws.ToInt32(client.lastList.MaxKeys) != 1) {
				t.Fatalf("invalid probe: %+v", client.lastList)
			}
			if tc.recursive && (result.key != "dir/" || result.destination != "local") {
				t.Fatal(result)
			}
			if tc.headErr == nil && result.head == nil {
				t.Fatal("successful HEAD was not retained")
			}
		})
	}
}
func TestAutomaticDirectoryDownloadAndUI(t *testing.T) {
	client := &detectionClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"}), headErr: statusError(404), objects: []types.Object{directoryObject("dir/file")}}
	request := downloadRequest{bucket: "bucket", key: "dir", destination: filepath.Join(t.TempDir(), "out"), partSize: 4, concurrency: 1}
	events := make(chan downloadProgress, 32)
	runDownload(context.Background(), client, request, events)
	var final downloadProgress
	for p := range events {
		final = p
	}
	if final.err != nil || !final.done || !final.recursive || final.completedFiles != 1 {
		t.Fatalf("%+v", final)
	}
	data, err := os.ReadFile(filepath.Join(request.destination, "file"))
	if err != nil || string(data) != "abc" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	model := newDownloadModel(client, request)
	defer model.cancel()
	updated, _ := model.Update(final)
	view := updated.(downloadModel).View().Content
	if !strings.Contains(view, "1/1") || !strings.Contains(view, "s3://bucket/dir/") {
		t.Fatal(view)
	}
	if client.heads != 2 || client.lists != 2 {
		t.Fatalf("heads=%d lists=%d", client.heads, client.lists)
	}
}
func TestDetectionSkipsExplicitDirectoryAndPresigned(t *testing.T) {
	for _, request := range []downloadRequest{{bucket: "bucket", key: "dir/", recursive: true}, {key: "dir"}} {
		client := &detectionClient{}
		if _, err := detectDownloadSource(context.Background(), client, request); err != nil {
			t.Fatal(err)
		}
		if client.heads != 0 || client.lists != 0 {
			t.Fatal("unexpected auto probe")
		}
	}
}
func TestDetectedFileReusesHead(t *testing.T) {
	client := &detectionClient{fakeRangeS3: newFakeRangeS3([]byte("abc"), map[string]string{"bytes=0-2": "abc"})}
	events := make(chan downloadProgress, 32)
	runDownload(context.Background(), client, downloadRequest{bucket: "bucket", key: "dir", destination: filepath.Join(t.TempDir(), "file"), partSize: 4, concurrency: 1}, events)
	for p := range events {
		if p.err != nil {
			t.Fatal(p.err)
		}
	}
	if client.heads != 1 || client.lists != 0 {
		t.Fatalf("heads=%d lists=%d", client.heads, client.lists)
	}
}
