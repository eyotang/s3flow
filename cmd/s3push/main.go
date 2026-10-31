package main

import (
	tea "charm.land/bubbletea/v2"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"io"
	"os"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run runs the independent s3push command.
func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseCLI(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printCLIUsage(stdout)
			return 0
		}
		fmt.Fprintf(stderr, "参数错误: %v\n", err)
		printCLIUsage(stderr)
		return 2
	}
	if opts.version {
		fmt.Fprintln(stdout, "s3push", update.Version)
		return 0
	}
	if update.BeforeRun("s3push") {
		return 0
	}
	bucket, files, err := planUpload(opts.source, opts.destination)
	if err != nil {
		fmt.Fprintf(stderr, "参数错误: %v\n", err)
		return 2
	}
	cfg, err := resolveConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "配置错误: %v\n", err)
		return 2
	}
	ctx := context.Background()
	client, err := pkg.NewS3Client(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "初始化上传客户端失败: %v\n", err)
		return 1
	}

	_, targetKey, err := pkg.ParseS3Location(opts.destination)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	sourceInfo, err := os.Lstat(opts.source)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	prefix := targetKey
	if sourceInfo.IsDir() {
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	} else if !strings.HasSuffix(prefix, "/") {
		if i := strings.LastIndex(prefix, "/"); i >= 0 {
			prefix = prefix[:i+1]
		} else {
			prefix = ""
		}
	}
	model := newUploadModel(client, uploadRequest{prefix: prefix, bucket: bucket, files: files, partSize: opts.partSize, concurrency: opts.concurrency, force: opts.force})
	defer model.cancel()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); runUpload(model.ctx, client, model.request, model.events) }()
	final, err := tea.NewProgram(model, tea.WithOutput(stdout)).Run()
	model.cancel()
	<-workerDone // Wait for multipart cleanup even if terminal initialization fails.
	if err != nil {
		if errors.Is(err, tea.ErrInterrupted) {
			return 130
		}
		fmt.Fprintf(stderr, "运行上传界面失败: %v\n", err)
		return 1
	}
	result, ok := final.(uploadModel)
	if !ok {
		fmt.Fprintln(stderr, "上传界面返回了未知状态")
		return 1
	}
	if result.err != nil {
		fmt.Fprintf(stderr, "上传失败: %v\n", result.err)
		if errors.Is(result.err, context.Canceled) {
			return 130
		}
		return 1
	}
	fmt.Fprintf(stdout, "上传完成，共 %d 个文件\n", len(files))
	return 0
}
