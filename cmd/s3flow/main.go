package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/eyotang/s3flow/internal/get"
	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/push"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
)

var errUpdated = errors.New("updated")

func newCommand() *cobra.Command {
	root := &cobra.Command{Use: "s3flow", Short: "S3 文件上传与下载", Version: update.Version, SilenceErrors: true, SilenceUsage: true}
	root.SetVersionTemplate("s3flow {{.Version}}\n")
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentPreRunE = func(c *cobra.Command, _ []string) error {
		if (c.Name() == "get" || c.Name() == "push") && update.BeforeRun("s3flow") {
			return errUpdated
		}
		return nil
	}
	root.AddCommand(get.NewCommand(), push.NewCommand())
	return root
}

func run(args []string, stdout, stderr io.Writer) int {
	root := newCommand()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	command, err := root.ExecuteC()
	if err == nil || errors.Is(err, errUpdated) {
		return 0
	}
	var exit pkg.ExitError
	if errors.As(err, &exit) {
		return int(exit)
	}
	fmt.Fprintf(stderr, "参数错误: %v\n\n", err)
	command.SetOut(stderr)
	command.Help()
	return 2
}
func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
