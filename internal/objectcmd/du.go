package objectcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/spf13/cobra"
)

type duOptions struct {
	summarize bool
	bytes     bool
	top       int
}

func newDUCommand() *cobra.Command {
	var bucket, prefix string
	var opts duOptions
	cmd := newClientCommand("du", "s3://bucket[/prefix/]", "Show object counts and storage usage by immediate child prefix", func(ctx context.Context, client *s3.Client, c *cobra.Command) error {
		return diskUsage(ctx, client, bucket, prefix, opts, c.OutOrStdout())
	})
	flags := cmd.Flags()
	flags.BoolVarP(&opts.summarize, "summarize", "s", false, "Show only the total for the requested prefix")
	flags.BoolVarP(&opts.bytes, "bytes", "b", false, "Print exact byte counts")
	flags.IntVar(&opts.top, "top", 0, "Show the N largest groups (0 shows all); the total includes every object")
	cmd.Args = func(c *cobra.Command, args []string) error {
		if err := cobra.ExactArgs(1)(c, args); err != nil {
			return err
		}
		if opts.top < 0 {
			return errors.New("--top must be zero or greater")
		}
		var err error
		bucket, prefix, err = pkg.ParseS3Location(args[0])
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		return err
	}
	return cmd
}

type usageTotal struct {
	bytes   big.Int
	objects uint64
}

func (t *usageTotal) add(size *big.Int) {
	t.bytes.Add(&t.bytes, size)
	t.objects++
}

func (t *usageTotal) formatSize(exact bool) string {
	if exact {
		return t.bytes.String()
	}
	if t.bytes.BitLen() <= 60 {
		return pkg.FormatBytes(t.bytes.Int64())
	}
	size, _ := t.bytes.Float64()
	return fmt.Sprintf("%.1f EiB", size/(1<<60))
}

func diskUsage(ctx context.Context, client pkg.ObjectLister, bucket, prefix string, opts duOptions, out io.Writer) error {
	var total usageTotal
	groups := make(map[string]*usageTotal)
	var size big.Int
	err := pkg.WalkObjects(ctx, client, bucket, prefix, func(object types.Object) error {
		key := aws.ToString(object.Key)
		relative, ok := strings.CutPrefix(key, prefix)
		if !ok || key == "" {
			return fmt.Errorf("invalid key %q in prefix listing", key)
		}
		if object.Size == nil || *object.Size < 0 {
			return fmt.Errorf("missing or invalid size for %s", pkg.FormatS3URIForDisplay(bucket, key))
		}
		if *object.Size == 0 && strings.HasSuffix(key, "/") {
			return nil
		}
		size.SetInt64(*object.Size)
		total.add(&size)
		if !opts.summarize {
			name := ""
			if slash := strings.IndexByte(relative, '/'); slash >= 0 {
				name = relative[:slash+1]
			}
			group := groups[name]
			if group == nil {
				group = new(usageTotal)
				groups[name] = group
			}
			group.add(&size)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	names := slices.Collect(maps.Keys(groups))
	slices.SortFunc(names, func(a, b string) int {
		if order := groups[b].bytes.Cmp(&groups[a].bytes); order != 0 {
			return order
		}
		return strings.Compare(a, b)
	})
	if opts.top > 0 && len(names) > opts.top {
		names = names[:opts.top]
	}

	w := bufio.NewWriter(out)
	column := "SIZE"
	if opts.bytes {
		column = "BYTES"
	}
	if _, err := fmt.Fprintf(w, "%s\tOBJECTS\tPREFIX\n", column); err != nil {
		return err
	}
	writeRow := func(row *usageTotal, key, label string) error {
		_, err := fmt.Fprintf(w, "%s\t%d\t%s%s\n", row.formatSize(opts.bytes), row.objects, pkg.FormatS3URIForDisplay(bucket, key), label)
		return err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		label := ""
		if name == "" {
			label = " (direct objects)"
		}
		if err := writeRow(groups[name], prefix+name, label); err != nil {
			return err
		}
	}
	if err := writeRow(&total, prefix, " (total)"); err != nil {
		return err
	}
	return w.Flush()
}
