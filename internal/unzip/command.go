// Package unzip extracts remote ZIP entries directly into S3 objects.
package unzip

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
)

type options struct {
	pkg.Options
	sourceBucket, sourceKey, bucket, prefix string
	concurrency, downloads, uploads, files  int
	partSize                                int64
	force                                   bool
}

// NewCommand builds unzip with shared connection flags and prints only total elapsed time on success.
func NewCommand() *cobra.Command {
	opts := options{Options: pkg.Options{PathStyle: true}}
	fs := flag.NewFlagSet("s3unzip", flag.ContinueOnError)
	pkg.AddConnectionFlags(fs, &opts.Options)
	fs.IntVar(&opts.concurrency, "concurrency", 4, "Default concurrent download and upload parts `N` (1-128)")
	fs.IntVar(&opts.downloads, "download-concurrency", 0, "Global concurrent Range requests `N` (default: concurrency)")
	fs.IntVar(&opts.uploads, "upload-concurrency", 0, "Global concurrent upload requests `N` (default: concurrency)")
	fs.IntVar(&opts.files, "file-concurrency", 2, "Concurrent ZIP entries `N` (1-128)")
	fs.BoolVar(&opts.force, "force", false, "Allow a non-empty destination prefix and overwrite matching objects")
	size := &pkg.ByteSizeFlag{Value: 8 << 20}
	fs.Var(size, "part-size", "Download and upload part size `SIZE` (5MiB-5GiB; default: 8MiB)")
	cmd := &cobra.Command{Use: "unzip [flags] <s3://bucket/archive.zip> <s3://bucket/prefix/>", Aliases: []string{"s3unzip"}, Short: "Stream ZIP entries from S3 directly to S3", Version: update.Version}
	pkg.AddCobraFlags(cmd.Flags(), fs)
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Fprintf(c.OutOrStdout(), "Usage:\n  %s [flags] <s3://bucket/archive.zip> <s3://bucket/prefix/>\n\nFlags:\n", c.CommandPath())
		pkg.PrintGroupedCobraFlags(c.OutOrStdout(), c.Flags())
	})
	cmd.Args = func(c *cobra.Command, args []string) error {
		if len(args) != 2 {
			return errors.New("an S3 ZIP source and an S3 destination prefix are required")
		}
		opts.partSize = size.Value
		if opts.downloads == 0 && !c.Flags().Changed("download-concurrency") {
			opts.downloads = opts.concurrency
		}
		if opts.uploads == 0 && !c.Flags().Changed("upload-concurrency") {
			opts.uploads = opts.concurrency
		}
		for _, n := range []int{opts.concurrency, opts.downloads, opts.uploads, opts.files} {
			if n < 1 || n > pkg.MaxConcurrency {
				return errors.New("concurrency must be 1-128")
			}
		}
		if opts.partSize < 5<<20 || opts.partSize > 5<<30 {
			return errors.New("part-size must be 5MiB-5GiB")
		}
		var err error
		opts.sourceBucket, opts.sourceKey, err = pkg.ParseS3URI(args[0])
		if err != nil {
			return err
		}
		opts.bucket, opts.prefix, err = pkg.ParseS3Location(args[1])
		if err != nil {
			return err
		}
		if opts.prefix != "" && !strings.HasSuffix(opts.prefix, "/") {
			opts.prefix += "/"
		}
		opts.CredentialsExplicit = c.Flags().Changed("ak") || c.Flags().Changed("sk") || c.Flags().Changed("session-token")
		opts.AWSDirectoryExplicit = c.Flags().Changed("aws")
		opts.AWSProfileExplicit = c.Flags().Changed("p") || c.Flags().Changed("profile")
		return nil
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		started := time.Now()
		cfg, err := pkg.ResolveConnectionConfig(opts.Options, pkg.ConnectionConfig(opts.Options))
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(c.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		client, err := newTransferClient(ctx, cfg, opts)
		if err == nil {
			err = extract(ctx, client, opts)
		}
		if err != nil {
			fmt.Fprintln(c.ErrOrStderr(), "解压失败:", err)
			if errors.Is(err, context.Canceled) {
				return pkg.ExitError(130)
			}
			return pkg.ExitError(1)
		}
		fmt.Fprintf(c.OutOrStdout(), "总耗时: %.3f 秒\n", time.Since(started).Seconds())
		return nil
	}
	return cmd
}

// Run executes the standalone s3unzip entrypoint and preserves native exit codes.
func Run(args []string, stdout, stderr io.Writer) int {
	cmd := NewCommand()
	cmd.Use = "s3unzip [flags] <s3://bucket/archive.zip> <s3://bucket/prefix/>"
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs(args)
	cmd.PreRunE = func(_ *cobra.Command, _ []string) error {
		if update.BeforeRun("s3unzip") {
			return errUpdated
		}
		return nil
	}
	err := cmd.Execute()
	if err == nil || errors.Is(err, errUpdated) {
		return 0
	}
	var exit pkg.ExitError
	if errors.As(err, &exit) {
		return int(exit)
	}
	fmt.Fprintln(stderr, "Argument error:", err)
	return 2
}

var errUpdated = errors.New("updated")
