package tree

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

	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
)

const usage = " [flags] s3://bucket[/prefix/]"

func NewCommand() *cobra.Command {
	var opts pkg.Options
	var bucket, prefix string
	var showSize bool
	flags := flag.NewFlagSet("s3tree", flag.ContinueOnError)
	pkg.AddConnectionFlags(flags, &opts)
	cmd := &cobra.Command{
		Use: "tree" + usage, Aliases: []string{"s3tree"},
		Short: "List an S3 bucket or prefix as a directory tree", Version: update.Version,
	}
	pkg.AddCobraFlags(cmd.Flags(), flags)
	cmd.Flags().BoolVarP(&showSize, "size", "s", false, "Show object sizes in human-readable units")
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	cmd.Args = func(c *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("one S3 bucket or prefix is required")
		}
		var err error
		bucket, prefix, err = pkg.ParseS3Location(args[0])
		if err != nil {
			return err
		}
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		flags := c.Flags()
		opts.CredentialsExplicit = flags.Changed("ak") || flags.Changed("sk") || flags.Changed("session-token")
		opts.AWSDirectoryExplicit = flags.Changed("aws")
		opts.AWSProfileExplicit = flags.Changed("p") || flags.Changed("profile")
		return nil
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		cfg, err := pkg.ResolveConnectionConfig(opts, pkg.ConnectionConfig(opts))
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(c.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		client, err := pkg.NewS3Client(ctx, cfg)
		if err == nil {
			err = printTree(ctx, client, bucket, prefix, showSize, c.OutOrStdout())
		}
		if err == nil {
			return nil
		}
		fmt.Fprintln(c.ErrOrStderr(), "s3tree:", err)
		if errors.Is(err, context.Canceled) {
			return pkg.ExitError(130)
		}
		return pkg.ExitError(1)
	}
	return cmd
}

func Run(args []string, stdout, stderr io.Writer) int {
	cmd := NewCommand()
	cmd.Use, cmd.Aliases = "s3tree"+usage, nil
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	if err == nil {
		return 0
	}
	var status pkg.ExitError
	if errors.As(err, &status) {
		return int(status)
	}
	fmt.Fprintln(stderr, "s3tree:", err)
	return 2
}
