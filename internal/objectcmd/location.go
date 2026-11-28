package objectcmd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
)

type location struct {
	bucket, path string
}

func parseLocation(arg string) (location, error) {
	if strings.HasPrefix(arg, "s3://") {
		bucket, key, err := pkg.ParseS3Location(arg)
		return location{bucket, key}, err
	}
	if arg == "" || strings.Contains(arg, "://") {
		return location{}, fmt.Errorf("expected a local path or s3:// URI, got %q", arg)
	}
	return location{path: arg}, nil
}

func (l location) remote() bool { return l.bucket != "" }
func (l location) stream() bool { return !l.remote() && l.path == "-" }
func (l location) isPrefix() bool {
	return l.remote() && (l.path == "" || strings.HasSuffix(l.path, "/"))
}

func (l location) String() string {
	if l.remote() {
		return pkg.FormatS3URIForDisplay(l.bucket, l.path)
	}
	return l.path
}

// A scope is a bucket prefix or an opened local directory. Names passed to a
// local root come from validated relative paths, including during deletion.
type scope struct {
	location
	root *os.Root
}

func (l location) split(recursive bool) (scope, string) {
	if recursive {
		if l.remote() && !l.isPrefix() {
			l.path += "/"
		}
		return scope{location: l}, ""
	}
	if l.remote() {
		key := l.path
		l.path = ""
		return scope{location: l}, key
	}
	name := filepath.Base(l.path)
	l.path = filepath.Dir(l.path)
	return scope{location: l}, name
}

func (l location) singleDestination(source location) (location, error) {
	name := filepath.Base(source.path)
	if source.remote() {
		name = source.path[strings.LastIndexByte(source.path, '/')+1:]
	}
	if l.isPrefix() {
		l.path += name
	} else if !l.remote() {
		info, err := os.Stat(l.path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return l, err
		}
		if (info != nil && info.IsDir()) || os.IsPathSeparator(l.path[len(l.path)-1]) {
			base, err := pkg.RelativeObjectPath("", name)
			if err != nil {
				return l, err
			}
			l.path = filepath.Join(l.path, base)
		}
	}
	return l, nil
}

func (s scope) object(name string) location {
	if s.remote() {
		return location{s.bucket, s.path + name}
	}
	return location{path: filepath.Join(s.path, filepath.FromSlash(name))}
}

func (s *scope) open(allowMissing bool) error {
	if s.remote() {
		return nil
	}
	root, err := os.OpenRoot(s.path)
	if allowMissing && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	s.root = root
	return err
}

func (s *scope) ensureDirectory() error {
	if s.remote() || s.root != nil {
		return nil
	}
	if err := os.MkdirAll(s.path, 0755); err != nil {
		return err
	}
	return s.open(false)
}

func (s *scope) close() {
	if s.root != nil {
		s.root.Close()
	}
}

type fileState struct {
	size     int64
	modified time.Time
	etag     *string
	local    fs.FileInfo
}

func localState(info fs.FileInfo) (fileState, error) {
	if !info.Mode().IsRegular() {
		return fileState{}, fmt.Errorf("expected a regular file, got %q (%s)", info.Name(), info.Mode().Type())
	}
	return fileState{size: info.Size(), modified: info.ModTime(), local: info}, nil
}

func (s scope) stat(ctx context.Context, client *s3.Client, name string) (fileState, error) {
	if s.remote() {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: aws.String(s.path + name)})
		if err != nil {
			return fileState{}, err
		}
		if head.ContentLength == nil || *head.ContentLength < 0 {
			return fileState{}, errors.New("S3 returned an invalid object size")
		}
		return fileState{size: *head.ContentLength, modified: aws.ToTime(head.LastModified), etag: head.ETag}, nil
	}
	info, err := s.root.Lstat(filepath.FromSlash(name))
	if err != nil {
		return fileState{}, err
	}
	return localState(info)
}

func (s scope) list(ctx context.Context, client *s3.Client) (map[string]fileState, error) {
	files := make(map[string]fileState)
	if s.remote() {
		err := pkg.WalkObjects(ctx, client, s.bucket, s.path, func(obj types.Object) error {
			key := aws.ToString(obj.Key)
			name, ok := strings.CutPrefix(key, s.path)
			if !ok || key == "" || obj.Size == nil || *obj.Size < 0 {
				return fmt.Errorf("invalid object listing for %q", key)
			}
			if strings.HasSuffix(key, "/") && *obj.Size == 0 {
				return nil
			}
			if _, exists := files[name]; exists {
				return fmt.Errorf("duplicate key in listing: %q", key)
			}
			files[name] = fileState{size: *obj.Size, modified: aws.ToTime(obj.LastModified), etag: obj.ETag}
			return nil
		})
		return files, err
	}
	if s.root == nil {
		return files, nil
	}
	err := fs.WalkDir(s.root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		files[name], err = localState(info)
		return err
	})
	return files, err
}

// Detect S3 keys that would escape or collapse onto the same local path.
func validateLocalNames(files map[string]fileState) error {
	names := make(map[string]bool, len(files))
	for key := range files {
		name, err := pkg.RelativeObjectPath("", key)
		if err != nil {
			return err
		}
		if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
			name = strings.ToLower(name)
		}
		if names[name] {
			return fmt.Errorf("multiple objects map to local path %q", name)
		}
		names[name] = true
	}
	for name := range names {
		for parent := filepath.Dir(name); parent != "."; parent = filepath.Dir(parent) {
			if names[parent] {
				return fmt.Errorf("object and directory share local path %q", parent)
			}
		}
	}
	return nil
}
