package unzip

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeS3 struct {
	mu                                                                    sync.Mutex
	archive                                                               []byte
	objects                                                               map[string][]byte
	parts                                                                 map[int32][]byte
	downloads, uploads, maxDownloads, maxUploads, gets, aborts, completes int
	badRange, changed, truncate, failUpload, nonempty, canceledAbort      bool
	transient                                                             int
	payloadDownloaded, firstUploadDownloaded                              int64
	allowOverwrite                                                        bool
	gate                                                                  chan struct{}
	putDelay                                                              time.Duration
}

// HeadObject describes a stable fake archive.
func (f *fakeS3) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(f.archive))), ETag: aws.String("\"archive\"")}, nil
}

// GetObject simulates concurrent ranges, retries, and response corruption.
func (f *fakeS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.IfMatch) != "\"archive\"" {
		return nil, errors.New("missing version pin")
	}
	var start, end int64
	if _, err := fmt.Sscanf(aws.ToString(in.Range), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.downloads++
	f.gets++
	f.maxDownloads = max(f.maxDownloads, f.downloads)
	transient := f.transient > 0
	if transient {
		f.transient--
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.downloads--; f.mu.Unlock() }()
	select {
	case <-time.After(2 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if transient {
		return nil, errors.New("transient network failure")
	}
	data := f.archive[start : end+1]
	if len(data) >= 1<<20 {
		f.mu.Lock()
		f.payloadDownloaded += int64(len(data))
		f.mu.Unlock()
	}
	contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.archive))
	if f.badRange {
		contentRange = ""
	}
	etag := "\"archive\""
	if f.changed {
		etag = "\"changed\""
	}
	if f.truncate && len(data) > 0 {
		data = data[:len(data)-1]
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(end - start + 1), ContentRange: aws.String(contentRange), ETag: aws.String(etag)}, nil
}

// ListObjectsV2 supplies the destination preflight.
func (f *fakeS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out := &s3.ListObjectsV2Output{}
	if f.nonempty {
		out.Contents = []types.Object{{Key: aws.String("out/existing"), Size: aws.Int64(1)}}
	}
	return out, nil
}

// PutObject records small validated objects, including empty objects with nil bodies.
func (f *fakeS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putDelay > 0 {
		f.mu.Lock()
		f.uploads++
		f.maxUploads = max(f.maxUploads, f.uploads)
		f.mu.Unlock()
		defer func() { f.mu.Lock(); f.uploads--; f.mu.Unlock() }()
		select {
		case <-time.After(f.putDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if (in.IfNoneMatch == nil) != f.allowOverwrite {
		return nil, errors.New("missing no-overwrite condition")
	}
	var data []byte
	var err error
	if in.Body != nil {
		data, err = io.ReadAll(in.Body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[*in.Key] = data
	return &s3.PutObjectOutput{}, err
}

// CreateMultipartUpload starts an isolated fake multipart session.
func (f *fakeS3) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("id")}, nil
}

// UploadPart delays workers to expose parallelism and supports failure/cancellation.
func (f *fakeS3) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	f.mu.Lock()
	f.uploads++
	if f.firstUploadDownloaded == 0 {
		f.firstUploadDownloaded = f.payloadDownloaded
	}
	f.maxUploads = max(f.maxUploads, f.uploads)
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.uploads--; f.mu.Unlock() }()
	if f.gate != nil {
		select {
		case f.gate <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(30 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.failUpload {
		return nil, errors.New("injected upload failure")
	}
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.parts[*in.PartNumber] = data
	f.mu.Unlock()
	return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprint(*in.PartNumber))}, nil
}

// CompleteMultipartUpload checks ordering and commits the assembled object.
func (f *fakeS3) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if (in.IfNoneMatch == nil) != f.allowOverwrite {
		return nil, errors.New("missing no-overwrite condition")
	}
	var result []byte
	for i, p := range in.MultipartUpload.Parts {
		if *p.PartNumber != int32(i+1) {
			return nil, errors.New("out of order")
		}
		result = append(result, f.parts[*p.PartNumber]...)
	}
	f.objects[*in.Key] = result
	f.completes++
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// AbortMultipartUpload records whether cleanup has a fresh uncanceled context.
func (f *fakeS3) AbortMultipartUpload(ctx context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts++
	f.canceledAbort = ctx.Err() != nil
	return &s3.AbortMultipartUploadOutput{}, nil
}

// makeArchive produces ZIPs with data descriptors using the selected compression method.
func makeArchive(t *testing.T, method uint16, files map[string][]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for name, data := range files {
		out, err := w.CreateHeader(&zip.FileHeader{Name: name, Method: method})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = out.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// fixture constructs bounded test settings and an in-memory S3 service.
func fixture(data []byte) (*fakeS3, options) {
	return &fakeS3{archive: data, objects: map[string][]byte{}, parts: map[int32][]byte{}}, options{sourceBucket: "source", sourceKey: "archive.zip", bucket: "dest", prefix: "out/", partSize: 5 << 20, downloads: 2, uploads: 2, files: 2}
}

// TestExtractStreamsAndParallelizes verifies byte equality, bounded concurrency, and upload before download completes.
func TestExtractStreamsAndParallelizes(t *testing.T) {
	big := make([]byte, 26<<20)
	_, _ = rand.New(rand.NewSource(7)).Read(big)
	for _, method := range []uint16{zip.Store, zip.Deflate} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			files := map[string][]byte{"big.bin": big, "nested/small.txt": []byte("hello"), "empty": {}}
			client, opts := fixture(makeArchive(t, method, files))
			if err := extract(context.Background(), client, opts); err != nil {
				t.Fatal(err)
			}
			for name, want := range files {
				got, ok := client.objects["out/"+name]
				if !ok || !bytes.Equal(got, want) {
					t.Fatalf("wrong output %s", name)
				}
			}
			if client.maxDownloads < 2 || client.maxDownloads > opts.downloads || client.maxUploads < 2 || client.maxUploads > opts.uploads {
				t.Fatalf("parallelism download=%d upload=%d", client.maxDownloads, client.maxUploads)
			}
			if client.firstUploadDownloaded >= int64(len(big)) {
				t.Fatal("upload waited for the entire payload download")
			}
			if client.completes != 1 || client.aborts != 0 {
				t.Fatalf("complete=%d abort=%d", client.completes, client.aborts)
			}
		})
	}
}

// TestBadRangesAndRetry rejects invalid responses and recovers from transient failures.
func TestBadRangesAndRetry(t *testing.T) {
	data := makeArchive(t, zip.Store, map[string][]byte{"file": []byte("data")})
	for _, kind := range []string{"range", "etag", "truncated", "retry"} {
		t.Run(kind, func(t *testing.T) {
			client, opts := fixture(data)
			switch kind {
			case "range":
				client.badRange = true
			case "etag":
				client.changed = true
			case "truncated":
				client.truncate = true
			case "retry":
				client.transient = 1
			}
			err := extract(context.Background(), client, opts)
			if kind == "retry" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || len(client.objects) != 0 {
				t.Fatalf("unsafe response accepted: %v", err)
			}
		})
	}
}

// TestRejectUnsafeEntriesBeforeWrites checks archive-wide validation precedes remote writes.
func TestRejectUnsafeEntriesBeforeWrites(t *testing.T) {
	for _, name := range []string{"../escape", "a/../escape", "/absolute", "a\\b", "C:/file", "./file", "a//b"} {
		t.Run(name, func(t *testing.T) {
			client, opts := fixture(makeArchive(t, zip.Store, map[string][]byte{name: []byte("bad"), "safe": []byte("safe")}))
			if err := extract(context.Background(), client, opts); err == nil || len(client.objects) != 0 {
				t.Fatalf("accepted %q: %v", name, err)
			}
		})
	}
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for i := 0; i < 2; i++ {
		out, _ := w.Create("duplicate")
		_, _ = out.Write([]byte("x"))
	}
	_ = w.Close()
	client, opts := fixture(b.Bytes())
	if err := extract(context.Background(), client, opts); err == nil {
		t.Fatal("accepted duplicates")
	}
}

// TestChecksumFailureAndUploadFailureAbort prevents corrupt commits and aborts incomplete uploads.
func TestChecksumFailureAndUploadFailureAbort(t *testing.T) {
	for _, kind := range []string{"small-crc", "large-crc", "upload"} {
		t.Run(kind, func(t *testing.T) {
			size := 10
			if kind != "small-crc" {
				size = 11 << 20
			}
			payload := bytes.Repeat([]byte("x"), size)
			data := makeArchive(t, zip.Store, map[string][]byte{"file": payload})
			if kind != "upload" {
				data[bytes.Index(data, []byte("xxxx"))] ^= 1
			}
			client, opts := fixture(data)
			client.failUpload = kind == "upload"
			err := extract(context.Background(), client, opts)
			if err == nil || len(client.objects) != 0 || client.completes != 0 {
				t.Fatalf("corrupt object committed: %v", err)
			}
			if kind != "small-crc" && client.aborts != 1 {
				t.Fatalf("missing abort: %d (%v)", client.aborts, err)
			}
		})
	}
}

// TestCancellationCleansUpAndJoinsWorkers verifies cancellation joins workers before uncanceled cleanup.
func TestCancellationCleansUpAndJoinsWorkers(t *testing.T) {
	client, opts := fixture(makeArchive(t, zip.Store, map[string][]byte{"file": bytes.Repeat([]byte("x"), 16<<20)}))
	client.gate = make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- extract(ctx, client, opts) }()
	select {
	case <-client.gate:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel deadlocked")
	}
	if client.aborts != 1 || client.canceledAbort || client.uploads != 0 || client.downloads != 0 {
		t.Fatalf("cleanup: %+v", client)
	}
}

// TestNonemptyDestinationAndSourceOverwrite protects existing destinations and the source archive.
func TestNonemptyDestinationAndSourceOverwrite(t *testing.T) {
	client, opts := fixture(makeArchive(t, zip.Store, map[string][]byte{"archive.zip": []byte("x")}))
	client.nonempty = true
	if err := extract(context.Background(), client, opts); err == nil || len(client.objects) != 0 {
		t.Fatal("nonempty destination accepted")
	}
	opts.bucket = opts.sourceBucket
	opts.prefix = ""
	opts.force = true
	if err := extract(context.Background(), client, opts); err == nil || !strings.Contains(err.Error(), "覆盖源 ZIP") {
		t.Fatal(err)
	}
}

// TestForceAllowsOverwrite verifies nonempty prefixes and conditional-write bypass are explicit.
func TestForceAllowsOverwrite(t *testing.T) {
	client, opts := fixture(makeArchive(t, zip.Store, map[string][]byte{"file": []byte("new")}))
	client.nonempty = true
	client.allowOverwrite = true
	opts.force = true
	if err := extract(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	if string(client.objects["out/file"]) != "new" {
		t.Fatal("missing forced upload")
	}
}

// TestDeclaredSizeMismatch rejects both short and oversized decompressed payloads.
func TestDeclaredSizeMismatch(t *testing.T) {
	for _, size := range []uint32{3, 5} {
		data := makeArchive(t, zip.Store, map[string][]byte{"file": []byte("data")})
		offset := bytes.Index(data, []byte{'P', 'K', 1, 2})
		binary.LittleEndian.PutUint32(data[offset+24:offset+28], size)
		client, opts := fixture(data)
		if err := extract(context.Background(), client, opts); err == nil || len(client.objects) != 0 {
			t.Fatalf("accepted size %d: %v", size, err)
		}
	}
}

// TestZIP64Index reads ZIP64 size extensions without allocating a large archive.
func TestZIP64Index(t *testing.T) {
	data := makeArchive(t, zip.Store, map[string][]byte{"file": []byte("data")})
	central := bytes.Index(data, []byte{'P', 'K', 1, 2})
	end := bytes.LastIndex(data, []byte{'P', 'K', 5, 6})
	extraOffset := central + 46 + int(binary.LittleEndian.Uint16(data[central+28:central+30]))
	extra := make([]byte, 20)
	binary.LittleEndian.PutUint16(extra[0:2], 1)
	binary.LittleEndian.PutUint16(extra[2:4], 16)
	binary.LittleEndian.PutUint64(extra[4:12], 4)
	binary.LittleEndian.PutUint64(extra[12:20], 4)
	binary.LittleEndian.PutUint32(data[central+20:central+24], 0xffffffff)
	binary.LittleEndian.PutUint32(data[central+24:central+28], 0xffffffff)
	binary.LittleEndian.PutUint16(data[central+30:central+32], uint16(len(extra)))
	binary.LittleEndian.PutUint32(data[end+12:end+16], binary.LittleEndian.Uint32(data[end+12:end+16])+uint32(len(extra)))
	result := append([]byte{}, data[:extraOffset]...)
	result = append(result, extra...)
	result = append(result, data[extraOffset:]...)
	client, opts := fixture(result)
	if err := extract(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	if string(client.objects["out/file"]) != "data" {
		t.Fatal("wrong ZIP64 output")
	}
}

// TestManySmallFilesShareRanges verifies ten thousand entries reuse aligned downloads.
// The injected PUT latency exercises file concurrency without requiring a real S3 bucket.
func TestManySmallFilesShareRanges(t *testing.T) {
	const count = 10000
	payload := make([]byte, 4096)
	_, _ = rand.New(rand.NewSource(11)).Read(payload)
	files := make(map[string][]byte, count)
	for i := 0; i < count; i++ {
		files[fmt.Sprintf("file-%05d", i)] = payload
	}
	client, opts := fixture(makeArchive(t, zip.Store, files))
	opts.files = 64
	opts.downloads = 16
	opts.uploads = 64
	opts.partSize = 8 << 20
	client.putDelay = 2 * time.Millisecond
	started := time.Now()
	if err := extract(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	expectedBlocks := (int64(len(client.archive)) + opts.partSize - 1) / opts.partSize
	if client.gets > int(expectedBlocks)+2 {
		t.Fatalf("GETs=%d; expected approximately %d shared ranges", client.gets, expectedBlocks)
	}
	if len(client.objects) != count {
		t.Fatalf("uploaded %d of %d", len(client.objects), count)
	}
	if client.maxUploads < 2 || client.maxUploads > opts.uploads {
		t.Fatalf("PUT concurrency %d", client.maxUploads)
	}
	for name, got := range client.objects {
		if !bytes.Equal(got, payload) {
			t.Fatalf("corrupt %s", name)
		}
	}
	t.Logf("files=%d archive=%.1fMiB GETs=%d maxPUTs=%d elapsed=%s (simulated S3, 2ms PUT delay)", count, float64(len(client.archive))/(1<<20), client.gets, client.maxUploads, time.Since(started))
}
