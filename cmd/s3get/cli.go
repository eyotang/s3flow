package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/eyotang/s3flow/internal/pkg"
	"io"
	"path"
	"path/filepath"
	"strings"
)

type cliOptions struct {
	pkg.Options
	version     bool
	source      string
	output      string
	partSize    int64
	concurrency int
	force       bool
	recursive   bool
}

type resolvedConfig struct {
	pkg.Config
	bucket        string
	key           string
	presignedURL  string
	sourceDisplay string
	destination   string
	partSize      int64
	concurrency   int
	force         bool
	recursive     bool
}

// parseCLI keeps argument handling independent from main so its precedence and
// validation rules can be tested without starting the terminal UI.
func parseCLI(args []string) (cliOptions, error) {
	opts := defaultCLIOptions()
	fs, partSize := newCLIFlagSet(&opts, io.Discard)
	if err := fs.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if opts.version {
		return opts, nil
	}
	fs.Visit(func(parsed *flag.Flag) {
		switch parsed.Name {
		case "ak", "sk", "session-token":
			opts.CredentialsExplicit = true
		case "aws":
			opts.AWSDirectoryExplicit = true
		case "p", "profile":
			opts.AWSProfileExplicit = true
		}
	})
	opts.partSize = partSize.Value
	positionals := fs.Args()
	if len(positionals) < 1 || len(positionals) > 2 {
		return cliOptions{}, errors.New("用法: s3get [参数] <s3://bucket/key|预签名URL> [输出文件]")
	}
	opts.source = positionals[0]
	if len(positionals) == 2 {
		if opts.output != "" {
			return cliOptions{}, errors.New("输出文件不能同时使用位置参数和 --output")
		}
		opts.output = positionals[1]
	}
	if opts.concurrency < 1 || opts.concurrency > pkg.MaxConcurrency {
		return cliOptions{}, fmt.Errorf("--concurrency 必须在 1 到 %d 之间", pkg.MaxConcurrency)
	}
	if opts.partSize <= 0 {
		return cliOptions{}, errors.New("--part-size 必须大于 0")
	}
	return opts, nil
}

func defaultCLIOptions() cliOptions {
	return cliOptions{Options: pkg.Options{PathStyle: true},
		partSize:    pkg.DefaultPartSize,
		concurrency: pkg.DefaultConcurrency,
	}
}

// newCLIFlagSet is the single source of truth for flags and their help text.
// ContinueOnError keeps parsing testable while still supporting the standard
// flag package's -h/--help behavior.
func newCLIFlagSet(opts *cliOptions, output io.Writer) (*flag.FlagSet, *pkg.ByteSizeFlag) {
	fs := flag.NewFlagSet("s3get", flag.ContinueOnError)
	fs.SetOutput(output)
	pkg.AddConnectionFlags(fs, &opts.Options)
	fs.BoolVar(&opts.version, "version", false, "显示当前版本并退出")
	fs.StringVar(&opts.output, "o", "", "输出文件 `FILE`")
	fs.StringVar(&opts.output, "output", "", "输出文件 `FILE`（等同 -o）")
	fs.IntVar(&opts.concurrency, "concurrency", pkg.DefaultConcurrency, "并发分片数 `N`，范围 1-128")
	fs.BoolVar(&opts.recursive, "recursive", false, "按目录前缀递归下载（等同 -r）")
	fs.BoolVar(&opts.recursive, "r", false, "按目录前缀递归下载")
	fs.BoolVar(&opts.force, "force", false, "覆盖已有输出文件")
	partSize := &pkg.ByteSizeFlag{Value: pkg.DefaultPartSize}
	fs.Var(partSize, "part-size", "分片大小 `SIZE`，例如 128MiB、1GiB")
	fs.Usage = func() {
		fmt.Fprintln(output, "用法:")
		fmt.Fprintln(output, "  s3get [参数] s3://bucket/key [输出文件]")
		fmt.Fprintln(output, "  s3get [参数] 'https://Endpoint/bucket/key?预签名参数' [输出文件]")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "参数（-flag 与 --flag 均可）:")
		fs.PrintDefaults()
	}
	return fs, partSize
}

func printCLIUsage(output io.Writer) {
	opts := defaultCLIOptions()
	fs, _ := newCLIFlagSet(&opts, output)
	fs.Usage()
}

// resolveConfig keeps direct CLI credentials highest. An explicit -p/--profile
// follows AWS CLI semantics and selects that profile's credentials; otherwise
// environment credentials precede the AWS shared files. Region and Endpoint
// precedence is delegated to the AWS SDK after applying explicit CLI values.
func resolveConfig(opts cliOptions) (resolvedConfig, error) {
	resolved := resolvedConfig{Config: pkg.ConnectionConfig(opts.Options), partSize: opts.partSize, concurrency: opts.concurrency, force: opts.force}

	raw := strings.TrimSpace(opts.source)
	if strings.HasPrefix(raw, "s3://") && (opts.recursive || strings.HasSuffix(raw, "/") || !strings.Contains(strings.TrimPrefix(raw, "s3://"), "/")) {
		bucket, key, err := pkg.ParseS3Location(raw)
		if err != nil {
			return resolvedConfig{}, err
		}
		if key != "" && !strings.HasSuffix(key, "/") {
			key += "/"
		}
		resolved.bucket, resolved.key, resolved.recursive = bucket, key, true
		resolved.sourceDisplay = pkg.FormatS3URIForDisplay(bucket, key)
		resolved.destination = opts.output
		if resolved.destination == "" {
			resolved.destination = path.Base(strings.TrimSuffix(key, "/"))
			if key == "" {
				resolved.destination = bucket
			}
		}
		resolved.Config, err = pkg.ResolveConnectionConfig(opts.Options, resolved.Config)
		return resolved, err
	}
	if opts.recursive {
		return resolvedConfig{}, errors.New("目录下载仅支持 S3 URI")
	}
	source, err := parseDownloadSource(opts.source)
	if err != nil {
		return resolvedConfig{}, err
	}
	resolved.bucket = source.bucket
	resolved.key = source.key
	resolved.presignedURL = source.presignedURL
	resolved.sourceDisplay = source.display
	resolved.destination = strings.TrimSpace(opts.output)
	if resolved.destination == "" {
		resolved.destination = source.filename
	}
	if resolved.destination == "" || resolved.destination == "." || resolved.destination == string(filepath.Separator) {
		return resolvedConfig{}, errors.New("无法推导输出文件名，请使用 --output 指定")
	}
	if source.presignedURL != "" {
		return resolved, nil
	}

	resolved.Config, err = pkg.ResolveConnectionConfig(opts.Options, resolved.Config)
	return resolved, err
}
