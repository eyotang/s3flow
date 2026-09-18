package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/eyotang/s3flow/internal/pkg"
)

type cliOptions struct {
	pkg.Options
	source      string
	destination string
	partSize    int64
	concurrency int
	force       bool
}

func defaultCLIOptions() cliOptions {
	return cliOptions{Options: pkg.Options{PathStyle: true}, partSize: pkg.DefaultPartSize, concurrency: pkg.DefaultConcurrency}
}
func newCLIFlagSet(opts *cliOptions, output io.Writer) (*flag.FlagSet, *pkg.ByteSizeFlag) {
	fs := flag.NewFlagSet("s3push", flag.ContinueOnError)
	fs.SetOutput(output)
	pkg.AddConnectionFlags(fs, &opts.Options)
	fs.IntVar(&opts.concurrency, "concurrency", pkg.DefaultConcurrency, "并发分片数 `N`，范围 1-128")
	fs.BoolVar(&opts.force, "force", false, "允许上传到非空目标目录并覆盖同名对象")
	partSize := &pkg.ByteSizeFlag{Value: pkg.DefaultPartSize}
	fs.Var(partSize, "part-size", "分片大小 `SIZE`，例如 128MiB、1GiB")
	fs.Usage = func() {
		fmt.Fprintln(output, "用法:")
		fmt.Fprintln(output, "  s3push [参数] <本地文件或目录> s3://bucket/key或prefix/")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "参数（-flag 与 --flag 均可）:")
		fs.PrintDefaults()
	}
	return fs, partSize
}

func parseCLI(args []string) (cliOptions, error) {
	opts := defaultCLIOptions()
	flags, size := newCLIFlagSet(&opts, io.Discard)
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "ak", "sk", "session-token":
			opts.CredentialsExplicit = true
		case "aws":
			opts.AWSDirectoryExplicit = true
		case "p", "profile":
			opts.AWSProfileExplicit = true
		}
	})
	opts.partSize = size.Value
	if flags.NArg() != 2 || opts.concurrency < 1 || opts.concurrency > pkg.MaxConcurrency || opts.partSize < 5<<20 || opts.partSize > 5<<30 {
		return cliOptions{}, errors.New("需要本地路径和 S3 目标；并发数为 1-128，上传分片大小为 5MiB-5GiB")
	}
	opts.source, opts.destination = flags.Arg(0), flags.Arg(1)
	return opts, nil
}

func printCLIUsage(output io.Writer) {
	opts := defaultCLIOptions()
	flags, _ := newCLIFlagSet(&opts, output)
	flags.Usage()
}

func resolveConfig(opts cliOptions) (pkg.Config, error) {
	return pkg.ResolveConnectionConfig(opts.Options, pkg.ConnectionConfig(opts.Options))
}
