package objectcmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/get"
	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/push"
)

type transfer struct {
	options
	client *s3.Client
	output io.Writer
}

func (t transfer) run(ctx context.Context, source, destination location, input io.Reader) error {
	if source.stream() || destination.stream() {
		return t.streamCopy(ctx, source, destination, input)
	}
	if !t.recursive {
		resolved, err := destination.singleDestination(source)
		if err != nil {
			return err
		}
		destination = resolved
	}
	src, sourceKey := source.split(t.recursive)
	dst, destKey := destination.split(t.recursive)
	if src.remote() && src.bucket == dst.bucket {
		if !t.recursive && source.path == destination.path {
			return errors.New("source and destination are the same object")
		}
		if t.recursive && (strings.HasPrefix(src.path, dst.path) || strings.HasPrefix(dst.path, src.path)) {
			return errors.New("source and destination prefixes overlap")
		}
	}
	if err := src.open(false); err != nil {
		return err
	}
	defer src.close()
	if err := dst.open(true); err != nil {
		return err
	}
	defer dst.close()
	if !t.recursive {
		state, err := src.stat(ctx, t.client, sourceKey)
		if err != nil {
			return err
		}
		return t.transferFile(ctx, src, &dst, sourceKey, destKey, state, nil)
	}
	sources, err := src.list(ctx, t.client)
	if err != nil {
		return err
	}
	if !dst.remote() {
		if err := validateLocalNames(sources); err != nil {
			return err
		}
	}
	var destinations map[string]fileState
	if t.name == "sync" {
		destinations, err = dst.list(ctx, t.client)
		if err != nil {
			return err
		}
	}
	for _, key := range slices.Sorted(maps.Keys(sources)) {
		state := sources[key]
		var previous *fileState
		if existing, found := destinations[key]; found {
			if !needsCopy(state, existing) {
				continue
			}
			previous = &existing
		}
		if err := t.transferFile(ctx, src, &dst, key, key, state, previous); err != nil {
			return err
		}
	}
	if t.deleteExtra {
		for _, key := range slices.Sorted(maps.Keys(destinations)) {
			if _, found := sources[key]; found {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !t.dryRun {
				if err := dst.remove(ctx, t.client, key, destinations[key]); err != nil {
					return fmt.Errorf("delete %s: %w", dst.object(key), err)
				}
			}
			if err := t.report("delete", dst.object(key).String()); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

func needsCopy(source, destination fileState) bool {
	return source.size != destination.size || source.modified.IsZero() || destination.modified.IsZero() ||
		source.modified.Truncate(time.Second).After(destination.modified.Truncate(time.Second))
}

func (t transfer) report(action, paths string) error {
	if t.dryRun {
		action = "(dryrun) " + action
	}
	_, err := fmt.Fprintf(t.output, "%s: %s\n", action, paths)
	return err
}

func (t transfer) transferFile(ctx context.Context, src scope, dst *scope, srcName, dstName string, state fileState, previous *fileState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	action := "copy"
	if t.name == "mv" {
		action = "move"
	}
	from, to := src.object(srcName), dst.object(dstName)
	paths := from.String() + " -> " + to.String()
	if t.dryRun {
		return t.report(action, paths)
	}
	if t.name == "mv" && src.remote() && !strongETag(state.etag) {
		return fmt.Errorf("cannot move %s without a source ETag", from)
	}
	if err := dst.ensureDirectory(); err != nil {
		return err
	}
	condition := pkg.WriteCondition{}
	if dst.remote() && !t.force {
		if previous == nil {
			condition.IfNoneMatch = aws.String("*")
		} else {
			if !strongETag(previous.etag) {
				return fmt.Errorf("cannot update %s without a destination ETag", to)
			}
			condition.IfMatch = previous.etag
		}
	}
	var err error
	switch {
	case !src.remote():
		err = t.upload(ctx, src, srcName, state, to, condition)
	case dst.remote():
		err = t.copyObject(ctx, from, to, state, condition)
	default:
		head := &s3.HeadObjectOutput{ContentLength: &state.size, ETag: state.etag}
		if !state.modified.IsZero() {
			head.LastModified = &state.modified
		}
		target := get.FileDestination{Root: dst.root, Name: filepath.FromSlash(dstName), Overwrite: t.force || previous != nil}
		if previous != nil {
			target.Previous = previous.local
		}
		err = get.DownloadFile(ctx, t.client, from.bucket, from.path, head, target, t.partSize.Value, t.concurrency)
	}
	if err != nil {
		return fmt.Errorf("copy %s: %w", paths, err)
	}
	if t.name == "mv" {
		if err := src.remove(ctx, t.client, srcName, state); err != nil {
			return fmt.Errorf("copied to %s, removing source %s failed: %w", to, from, err)
		}
	}
	return t.report(action, paths)
}

func (t transfer) upload(ctx context.Context, src scope, name string, state fileState, to location, condition pkg.WriteCondition) error {
	file, err := src.root.Open(filepath.FromSlash(name))
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !pkg.SameFileState(state.local, info) {
		return errors.New("local source changed before upload")
	}
	if err := push.UploadFile(ctx, t.client, to.bucket, to.path, file, t.partSize.Value, t.concurrency, condition); err != nil {
		return err
	}
	info, err = file.Stat()
	if err != nil {
		return err
	}
	if !pkg.SameFileState(state.local, info) {
		return errors.New("local source changed during upload")
	}
	return nil
}

func strongETag(etag *string) bool {
	return etag != nil && *etag != "" && *etag != "*" && !strings.HasPrefix(strings.ToUpper(*etag), "W/")
}

func (s scope) remove(ctx context.Context, client *s3.Client, name string, state fileState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.remote() {
		if !strongETag(state.etag) {
			return errors.New("cannot delete an object without its ETag")
		}
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.path + name), IfMatch: state.etag})
		return err
	}
	path := filepath.FromSlash(name)
	info, err := s.root.Lstat(path)
	if err != nil {
		return err
	}
	if !pkg.SameFileState(state.local, info) {
		return errors.New("local file changed before deletion")
	}
	return s.root.Remove(path)
}

func (t transfer) streamCopy(ctx context.Context, from, to location, input io.Reader) error {
	paths := from.String() + " -> " + to.String()
	if t.dryRun {
		return t.report("copy", paths)
	}
	if to.stream() {
		result, err := t.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &from.bucket, Key: &from.path})
		if err != nil {
			return err
		}
		defer result.Body.Close()
		_, err = io.Copy(t.output, result.Body)
		return err
	}
	// Stage stdin to provide a seekable source for SDK retries and multipart uploads.
	file, err := os.CreateTemp("", "s3cp-stdin-*")
	if err != nil {
		return err
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	if stdin, ok := input.(*os.File); ok && stdin == os.Stdin {
		stop := context.AfterFunc(ctx, func() { stdin.Close() })
		defer stop()
	}
	if _, err := io.Copy(file, input); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	condition := pkg.WriteCondition{}
	if !t.force {
		condition.IfNoneMatch = aws.String("*")
	}
	if err := push.UploadFile(ctx, t.client, to.bucket, to.path, file, t.partSize.Value, t.concurrency, condition); err != nil {
		return err
	}
	return t.report("copy", paths)
}
