package rg

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
)

type searchClient interface {
	pkg.ObjectLister
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (t target) uri() string { return pkg.FormatS3URIForDisplay(t.bucket, t.key) }

func search(parent context.Context, client searchClient, opts options, stdout, stderr io.Writer) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	type job struct {
		target
		showURI bool
	}
	jobs := make(chan job)
	var workers sync.WaitGroup
	var mu sync.Mutex
	var matched, failed bool
	report := func(t target, err error) {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() == nil {
			failed = true
			fmt.Fprintf(stderr, "s3rg: %s: %v\n", t.uri(), err)
		}
	}
	emit := func(data []byte) error {
		mu.Lock()
		defer mu.Unlock()
		n, err := stdout.Write(data)
		if err == nil && n != len(data) {
			return io.ErrShortWrite
		}
		return err
	}
	for range opts.jobs {
		workers.Go(func() {
			for entry := range jobs {
				if ctx.Err() != nil {
					return
				}
				found, err := scanObject(ctx, client, entry.target, opts, entry.showURI, emit)
				if found {
					mu.Lock()
					matched = true
					mu.Unlock()
					if opts.quiet {
						cancel()
					}
				}
				if err != nil {
					report(entry.target, err)
				}
			}
		})
	}
	idx := newSearchIndex(client, opts, func(place target, err error) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(stderr, "s3rg: index at %s is not used: %v; searching without it\n", place.uri(), err)
	})
	seen := make(map[target]bool)
	for _, t := range opts.targets {
		if ctx.Err() != nil {
			break
		}
		places := ancestorIndexes(t)
		addIndex := func(place target) { places[place] = true }
		submit := func(object objectMeta, showURI bool) error {
			if seen[object.target] {
				return nil
			}
			seen[object.target] = true
			if idx.skip(ctx, places, object, objectDecoding(object.key, opts.noDecompress)) {
				return nil
			}
			select {
			case jobs <- job{object.target, showURI}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := discover(ctx, client, t, opts.globs, len(opts.targets) > 1, addIndex, submit)
		if err != nil {
			report(t, err)
		}
	}
	close(jobs)
	workers.Wait()
	if opts.indexStats {
		idx.printStats(stderr)
	}
	switch {
	case opts.quiet && matched:
		return nil
	case parent.Err() != nil:
		fmt.Fprintln(stderr, "s3rg:", parent.Err())
		return pkg.ExitError(2)
	case failed:
		return pkg.ExitError(2)
	case matched:
		return nil
	default:
		return pkg.ExitError(1)
	}
}

func discover(ctx context.Context, client searchClient, t target, globs []objectGlob, multiple bool, addIndex func(target), submit func(objectMeta, bool) error) error {
	prefix := t.key
	var probeErr error
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		var head *s3.HeadObjectOutput
		head, probeErr = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)})
		if probeErr == nil {
			return submit(objectMeta{t, head.ContentLength, head.ETag}, multiple)
		}
		if !s3Missing(probeErr) && !s3Status(probeErr, 403) {
			return probeErr
		}
		prefix += "/"
	}
	found, err := walkSearchObjects(ctx, client, target{t.bucket, prefix}, addIndex, func(object objectMeta, relative string) error {
		if !accepts(globs, relative) {
			return nil
		}
		return submit(object, true)
	})
	if err != nil {
		return err
	}
	if !found {
		return probeErr
	}
	return nil
}

func walkSearchObjects(ctx context.Context, client searchClient, prefix target, addIndex func(target), visit func(objectMeta, string) error) (bool, error) {
	var found bool
	markers := make(map[target]bool)
	err := pkg.WalkObjects(ctx, client, prefix.bucket, prefix.key, func(item types.Object) error {
		key := aws.ToString(item.Key)
		relative, under := strings.CutPrefix(key, prefix.key)
		if key == "" || !under {
			return fmt.Errorf("invalid key %q in prefix listing", key)
		}
		found = true
		if strings.HasSuffix(key, "/") && aws.ToInt64(item.Size) == 0 {
			return nil
		}
		object := objectMeta{target{prefix.bucket, key}, item.Size, item.ETag}
		if strings.Contains(relative, indexDir) {
			if place, marked := markedIndex(ctx, client, markers, object.target, relative); marked {
				if addIndex != nil {
					addIndex(place)
				}
				return nil
			}
		}
		return visit(object, relative)
	})
	return found, err
}

const maxLine = 16 << 20

const (
	decodeRaw = iota
	decodeGzip
)

func objectDecoding(key string, raw bool) uint8 {
	if !raw {
		switch strings.ToLower(path.Ext(key)) {
		case ".gz", ".gzip":
			return decodeGzip
		}
	}
	return decodeRaw
}

func decodeContent(reader io.Reader, decoding uint8) (io.Reader, func(), error) {
	if decoding == decodeGzip {
		decoder, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("gzip: %w", err)
		}
		return decoder, func() { decoder.Close() }, nil
	}
	return reader, func() {}, nil
}

func disableMissingChecksumWarning(options *s3.Options) {
	options.DisableLogOutputChecksumValidationSkipped = true
}

func scanObject(ctx context.Context, client searchClient, t target, opts options, showURI bool, emit func([]byte) error) (matchedObject bool, searchErr error) {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)}, disableMissingChecksumWarning)
	if err != nil {
		return false, err
	}
	defer result.Body.Close()
	reader, finish, err := decodeContent(result.Body, objectDecoding(t.key, opts.noDecompress))
	if err != nil {
		return false, err
	}
	defer finish()
	batch := lineBatch{write: emit}
	if !opts.count && !opts.files && !opts.quiet {
		reader = flushBeforeRead{Reader: reader, flush: batch.flush}
		defer func() {
			if err := batch.flush(); err != nil {
				searchErr = errors.Join(searchErr, err)
			}
		}()
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 32<<10), maxLine+2)
	prefix := ""
	if showURI {
		prefix = t.uri() + ":"
	}
	var count, lineNumber int64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return count > 0, err
		}
		lineNumber++
		line := scanner.Bytes()
		if len(line) > maxLine {
			return count > 0, errors.New("line exceeds 16 MiB")
		}
		if bytes.IndexByte(line, 0) >= 0 {
			return count > 0, errors.New("line contains a NUL byte")
		}
		matched := false
		for _, match := range opts.matchers {
			if match(line) {
				matched = true
				break
			}
		}
		if matched == opts.invert {
			continue
		}
		count++
		switch {
		case opts.quiet:
			return true, nil
		case opts.files:
			return true, emit([]byte(t.uri() + "\n"))
		case opts.count:
			continue
		case opts.numbers:
			err = batch.add(prefix, lineNumber, line)
		default:
			err = batch.add(prefix, 0, line)
		}
		if err != nil {
			return true, err
		}
	}
	if err := scanner.Err(); err != nil {
		return count > 0, err
	}
	if opts.count && count > 0 {
		return true, emit(append(strconv.AppendInt([]byte(prefix), count, 10), '\n'))
	}
	return count > 0, nil
}
