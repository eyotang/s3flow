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
	emit := func(record string) error {
		mu.Lock()
		defer mu.Unlock()
		_, err := io.WriteString(stdout, record+"\n")
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
	seen := make(map[target]bool)
	submit := func(t target, showURI bool) error {
		if seen[t] {
			return nil
		}
		seen[t] = true
		select {
		case jobs <- job{t, showURI}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, t := range opts.targets {
		if ctx.Err() != nil {
			break
		}
		err := discover(ctx, client, t, opts.globs, len(opts.targets) > 1, submit)
		if err != nil {
			report(t, err)
		}
	}
	close(jobs)
	workers.Wait()
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

func discover(ctx context.Context, client searchClient, t target, globs []string, multiple bool, submit func(target, bool) error) error {
	prefix := t.key
	var probeErr error
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		_, probeErr = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)})
		if probeErr == nil {
			return submit(t, multiple)
		}
		var response interface{ HTTPStatusCode() int }
		var notFound *types.NotFound
		if !errors.As(probeErr, &notFound) && (!errors.As(probeErr, &response) || (response.HTTPStatusCode() != 403 && response.HTTPStatusCode() != 404)) {
			return probeErr
		}
		prefix += "/"
	}
	found := false
	err := pkg.WalkObjects(ctx, client, t.bucket, prefix, func(object types.Object) error {
		key := aws.ToString(object.Key)
		if key == "" || !strings.HasPrefix(key, prefix) {
			return fmt.Errorf("invalid key %q in prefix listing", key)
		}
		found = true
		if strings.HasSuffix(key, "/") && aws.ToInt64(object.Size) == 0 {
			return nil
		}
		if !accepts(globs, strings.TrimPrefix(key, prefix)) {
			return nil
		}
		return submit(target{t.bucket, key}, true)
	})
	if err != nil {
		return err
	}
	if !found {
		return probeErr
	}
	return nil
}

func accepts(globs []string, key string) bool {
	includes, matched := false, false
	for _, glob := range globs {
		pattern := strings.TrimPrefix(glob, "!")
		name := key
		if !strings.Contains(pattern, "/") {
			name = path.Base(key)
		}
		match, _ := path.Match(pattern, name)
		if pattern != glob {
			if match {
				return false
			}
		} else {
			includes = true
			matched = matched || match
		}
	}
	return !includes || matched
}

const maxLine = 16 << 20

func scanObject(ctx context.Context, client searchClient, t target, opts options, showURI bool, emit func(string) error) (bool, error) {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)}, func(options *s3.Options) {
		options.DisableLogOutputChecksumValidationSkipped = true
	})
	if err != nil {
		return false, err
	}
	defer result.Body.Close()
	var reader io.Reader = result.Body
	switch strings.ToLower(path.Ext(t.key)) {
	case ".gz", ".gzip":
		decoder, err := gzip.NewReader(reader)
		if err != nil {
			return false, fmt.Errorf("gzip: %w", err)
		}
		defer decoder.Close()
		reader = decoder
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
		for _, expression := range opts.expressions {
			if expression.Match(line) {
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
			return true, emit(t.uri())
		case opts.count:
			continue
		case opts.numbers:
			err = emit(fmt.Sprintf("%s%d:%s", prefix, lineNumber, line))
		default:
			err = emit(prefix + string(line))
		}
		if err != nil {
			return true, err
		}
	}
	if err := scanner.Err(); err != nil {
		return count > 0, err
	}
	if opts.count && count > 0 {
		return true, emit(fmt.Sprintf("%s%d", prefix, count))
	}
	return count > 0, nil
}
