package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	tea "charm.land/bubbletea/v2"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	options, err := parseCLI(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printCLIUsage(stdout)
			return 0
		}
		fmt.Fprintf(stderr, "参数错误: %v\n\n", err)
		printCLIUsage(stderr)
		return 2
	}
	configuration, err := resolveConfig(options)
	if err != nil {
		fmt.Fprintf(stderr, "配置错误: %v\n", err)
		return 2
	}
	client, err := newDownloadClient(context.Background(), configuration)
	if err != nil {
		fmt.Fprintf(stderr, "初始化下载客户端失败: %v\n", err)
		return 1
	}
	request := downloadRequest{
		bucket:        configuration.bucket,
		key:           configuration.key,
		sourceDisplay: configuration.sourceDisplay,
		destination:   configuration.destination,
		partSize:      configuration.partSize,
		concurrency:   configuration.concurrency,
		force:         configuration.force,
		recursive:     configuration.recursive,
	}
	model := newDownloadModel(client, request)
	defer model.cancel()
	final, err := tea.NewProgram(model).Run()
	if err != nil {
		if errors.Is(err, tea.ErrInterrupted) {
			return 130
		}
		fmt.Fprintf(stderr, "运行下载界面失败: %v\n", err)
		return 1
	}
	result, ok := final.(downloadModel)
	if !ok {
		fmt.Fprintln(stderr, "下载界面返回了未知状态")
		return 1
	}
	if result.err != nil {
		if errors.Is(result.err, context.Canceled) {
			return 130
		}
		fmt.Fprintf(stderr, "下载失败: %v\n", result.err)
		return 1
	}
	fmt.Fprintf(stdout, "下载完成: %s\n", configuration.destination)
	return 0
}
