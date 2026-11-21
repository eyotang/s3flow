package tree

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
)

// Directory names end in a slash, so an object named "logs" can coexist
// with the directory "logs/" in the same tree.
type directory map[string]entry

type entry struct {
	children directory
	size     *int64
}

func (d directory) add(key string, marker bool, size *int64) {
	for {
		name, rest, hasSlash := strings.Cut(key, "/")
		if !hasSlash {
			d[name] = entry{size: size}
			return
		}
		name += "/"
		child := d[name].children
		if child == nil {
			child = make(directory)
			d[name] = entry{children: child}
		}
		if rest == "" && marker {
			return
		}
		d, key = child, rest
	}
}

func printTree(ctx context.Context, client pkg.ObjectLister, bucket, prefix string, showSize bool, out io.Writer) error {
	root := make(directory)
	err := pkg.WalkObjects(ctx, client, bucket, prefix, func(object types.Object) error {
		key := aws.ToString(object.Key)
		relative, under := strings.CutPrefix(key, prefix)
		if key == "" || !under {
			return fmt.Errorf("invalid key %q in prefix listing", key)
		}
		marker := strings.HasSuffix(key, "/") && aws.ToInt64(object.Size) == 0
		if relative != "" || !marker {
			root.add(relative, marker, object.Size)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	output := bufio.NewWriter(out)
	if _, err := fmt.Fprintln(output, pkg.FormatS3URIForDisplay(bucket, prefix)); err != nil {
		return err
	}
	var count totals
	if err := root.write(ctx, output, "", showSize, &count); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "\n%d %s, %d %s\n", count.directories, plural(count.directories, "directory", "directories"), count.objects, plural(count.objects, "object", "objects")); err != nil {
		return err
	}
	return output.Flush()
}

type totals struct{ directories, objects int }

func (d directory) write(ctx context.Context, out io.Writer, indent string, showSize bool, count *totals) error {
	names := slices.Sorted(maps.Keys(d))
	for i, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		branch, next := "├── ", "│   "
		if i == len(names)-1 {
			branch, next = "└── ", "    "
		}
		item := d[name]
		label := displayName(strings.TrimSuffix(name, "/"))
		if item.children != nil {
			label += "/"
		} else if showSize {
			size := "unknown size"
			if item.size != nil && *item.size >= 0 {
				size = pkg.FormatBytes(*item.size)
			}
			label += "  [" + size + "]"
		}
		if _, err := fmt.Fprintln(out, indent+branch+label); err != nil {
			return err
		}
		if item.children == nil {
			count.objects++
			continue
		}
		count.directories++
		if err := item.children.write(ctx, out, indent+next, showSize, count); err != nil {
			return err
		}
	}
	return nil
}

func displayName(name string) string {
	if name == "" || strings.ContainsFunc(name, func(r rune) bool { return !unicode.IsPrint(r) || r == '"' || r == '\\' }) {
		return strconv.Quote(name)
	}
	return name
}

func plural(n int, singular, multiple string) string {
	if n == 1 {
		return singular
	}
	return multiple
}
