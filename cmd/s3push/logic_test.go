package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type uploadStub struct {
	mu                                    sync.Mutex
	chunks                                map[int32][]byte
	body                                  []byte
	fail, failComplete, aborted, finished bool
	condition                             string
	cancel                                context.CancelFunc
}

func (s *uploadStub) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.body, _ = io.ReadAll(in.Body)
	s.condition = aws.ToString(in.IfNoneMatch)
	return &s3.PutObjectOutput{}, nil
}
func (s *uploadStub) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("id")}, nil
}
func (s *uploadStub) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if s.cancel != nil {
		s.cancel()
		return nil, ctx.Err()
	}
	if s.fail {
		return nil, errors.New("part failed")
	}
	data, err := io.ReadAll(in.Body)
	s.mu.Lock()
	s.chunks[*in.PartNumber] = data
	s.mu.Unlock()
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, err
}
func (s *uploadStub) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if s.failComplete {
		return nil, errors.New("conditional write failed")
	}
	s.finished = true
	s.condition = aws.ToString(in.IfNoneMatch)
	for i, part := range in.MultipartUpload.Parts {
		if *part.PartNumber != int32(i+1) {
			return nil, errors.New("parts out of order")
		}
		s.body = append(s.body, s.chunks[*part.PartNumber]...)
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (s *uploadStub) AbortMultipartUpload(ctx context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	s.aborted = true
	return &s3.AbortMultipartUploadOutput{}, ctx.Err()
}
func TestUploadObject(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		size                              int
		force, fail, failComplete, cancel bool
	}{
		{name: "empty"}, {name: "small", size: 42}, {name: "overwrite", size: 42, force: true},
		{name: "multipart", size: 11 << 20}, {name: "multipart overwrite", size: 11 << 20, force: true},
		{name: "failed part", size: 11 << 20, fail: true}, {name: "failed completion", size: 11 << 20, failComplete: true},
		{name: "cancel", size: 11 << 20, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			for i := range data {
				data[i] = byte(i % 251)
			}
			name := filepath.Join(t.TempDir(), "source")
			if err := os.WriteFile(name, data, 0600); err != nil {
				t.Fatal(err)
			}
			stub := &uploadStub{chunks: map[int32][]byte{}, fail: tc.fail, failComplete: tc.failComplete}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				stub.cancel = cancel
			}
			err := uploadObject(ctx, stub, "bucket", uploadFile{name, "key"}, 5<<20, 3, tc.force, nil)
			if tc.fail || tc.failComplete || tc.cancel {
				if err == nil || !stub.aborted || stub.finished {
					t.Fatalf("err=%v aborted=%v finished=%v", err, stub.aborted, stub.finished)
				}
				if tc.cancel && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, stub.body) || stub.aborted {
				t.Fatal("data mismatch or aborted success")
			}
			want := "*"
			if tc.force {
				want = ""
			}
			if stub.condition != want {
				t.Fatalf("condition=%q", stub.condition)
			}
		})
	}
}
func TestPlanUpload(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(nested, "中文+.txt")
	if err := os.WriteFile(name, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ local, target, key string }{
		{dir, "s3://bucket/prefix/", "prefix/nested/中文+.txt"},
		{dir, "s3://bucket/prefix", "prefix/nested/中文+.txt"},
		{dir, "s3://bucket", "nested/中文+.txt"},
		{name, "s3://bucket/", "中文+.txt"},
		{name, "s3://bucket/%E4%B8%AD%E6%96%87", "中文"},
	} {
		bucket, files, err := planUpload(tc.local, tc.target)
		if err != nil || bucket != "bucket" || len(files) != 1 || files[0].key != tc.key {
			t.Fatalf("%s: %s %+v %v", tc.target, bucket, files, err)
		}
	}
	if err := os.Symlink(name, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := planUpload(dir, "s3://bucket/"); err == nil {
		t.Fatal("accepted symlink")
	}
}

func TestUploadReaderRewindDoesNotDoubleCount(t *testing.T) {
	var latest objectProgress
	tracker := &uploadTracker{report: func(p objectProgress) { latest = p }}
	reader := &uploadReader{reader: io.NewSectionReader(bytes.NewReader([]byte("abcdef")), 0, 6), tracker: tracker}
	if _, err := reader.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if latest.transferred != 0 {
		t.Fatal("size probe counted bytes")
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, reader, 3); err != nil {
		t.Fatal(err)
	}
	if latest.transferred != 3 {
		t.Fatal(latest)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if latest.transferred != 0 {
		t.Fatal("retry did not reset bytes")
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatal(err)
	}
	if latest.transferred != 6 {
		t.Fatal(latest)
	}
}

func TestRunUploadReportsDirectoryTotalsAndTerminalState(t *testing.T) {
	dir := t.TempDir()
	var files []uploadFile
	for i, name := range []string{"a", "b"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, make([]byte, i+1), 0600); err != nil {
			t.Fatal(err)
		}
		files = append(files, uploadFile{path, name})
	}
	events := make(chan uploadProgress, 1) // A slow/stopped UI must not block completion.
	runUpload(context.Background(), &uploadStub{}, uploadRequest{bucket: "bucket", files: files, partSize: 5 << 20, concurrency: 2}, events)
	final := <-events
	if !final.done || final.err != nil || final.total != 3 || final.uploaded != 3 || final.completedFiles != 2 || final.totalFiles != 2 {
		t.Fatalf("unexpected terminal progress: %+v", final)
	}
}

func TestRunUploadCancellationPublishesAfterAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(path, make([]byte, 6<<20), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &uploadStub{cancel: cancel}
	events := make(chan uploadProgress, 1)
	runUpload(ctx, client, uploadRequest{bucket: "bucket", files: []uploadFile{{path, "key"}}, partSize: 5 << 20, concurrency: 2}, events)
	final := <-events
	if !client.aborted || !final.done || !errors.Is(final.err, context.Canceled) || final.completedFiles != 0 {
		t.Fatalf("unexpected cancellation: %+v", final)
	}
}

func TestPartProgressSnapshotsAreIndependent(t *testing.T) {
	var snapshots []objectProgress
	tracker := &uploadTracker{report: func(p objectProgress) { snapshots = append(snapshots, p) }}
	tracker.startPart(1, 10)
	tracker.partBytes(1, 5)
	tracker.finishPart(1)
	if snapshots[0].parts[0].transferred != 0 || !snapshots[0].parts[0].finishedAt.IsZero() {
		t.Fatal("a published snapshot was mutated")
	}
	final := snapshots[len(snapshots)-1]
	if final.completedParts != 1 || final.parts[0].transferred != 5 || final.parts[0].finishedAt.IsZero() {
		t.Fatal(final)
	}
}

func (s *uploadStub) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

type prefixUploadStub struct {
	uploadStub
	pages    []*s3.ListObjectsV2Output
	listErr  error
	calls    int
	prefixes []string
}

func (s *prefixUploadStub) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	s.prefixes = append(s.prefixes, aws.ToString(in.Prefix))
	s.calls++
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.pages[s.calls-1], nil
}
func TestUploadChecksWholePrefixBeforeWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new")
	if err := os.WriteFile(path, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		force, deny bool
		pages       []*s3.ListObjectsV2Output
		listErr     error
	}{
		{name: "unrelated object", deny: true, pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("dir/other"), Size: aws.Int64(1)}}}}},
		{name: "folder marker then occupied page", deny: true, pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("dir/"), Size: aws.Int64(0)}}, IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next")}, {Contents: []types.Object{{Key: aws.String("dir/nested/other"), Size: aws.Int64(1)}}}}},
		{name: "empty marker", pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("dir/"), Size: aws.Int64(0)}}}}},
		{name: "list denied", deny: true, listErr: errors.New("access denied")},
		{name: "force", force: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &prefixUploadStub{pages: tc.pages, listErr: tc.listErr}
			events := make(chan uploadProgress, 1)
			runUpload(context.Background(), client, uploadRequest{bucket: "bucket", prefix: "dir/", files: []uploadFile{{path, "dir/new"}}, partSize: 5 << 20, concurrency: 1, force: tc.force}, events)
			final := <-events
			if (final.err != nil) != tc.deny {
				t.Fatalf("error: %v", final.err)
			}
			if tc.deny && len(client.body) > 0 {
				t.Fatal("wrote before prefix validation")
			}
			if !tc.deny && string(client.body) != "new" {
				t.Fatal("missing upload")
			}
			if tc.force && client.calls != 0 {
				t.Fatal("force should bypass empty check")
			}
			for _, prefix := range client.prefixes {
				if prefix != "dir/" {
					t.Fatal(prefix)
				}
			}
		})
	}
}

func TestPlanUploadAcceptsZeroByteFiles(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(name, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{dir, name} {
		_, files, err := planUpload(source, "s3://bucket/prefix/")
		if err != nil || len(files) != 1 || files[0].local != name {
			t.Fatalf("files=%v err=%v", files, err)
		}
	}
}
