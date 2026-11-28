package objectcmd

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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type options struct {
	name                                  string
	recursive, dryRun, force, deleteExtra bool
	concurrency                           int
	partSize                              pkg.ByteSizeFlag
}

func NewCommand(name string) *cobra.Command {
	descriptions := map[string]string{
		"sync":    "Copy new or changed files between directories and S3 prefixes",
		"cp":      "Copy a file, object, or prefix locally or in S3",
		"mv":      "Move files or objects by copying each one before removing its source",
		"presign": "Print a time-limited S3 GET URL",
	}
	opts := options{name: name, partSize: pkg.ByteSizeFlag{Value: pkg.DefaultPartSize}}
	var connection pkg.Options
	var source, destination location
	var expires int
	cmd := &cobra.Command{
		Use: name + " [flags] SOURCE DESTINATION", Short: descriptions[name],
		Aliases: []string{"s3" + name}, Version: update.Version,
	}
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	flags := cmd.Flags()
	flags.SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
		switch name {
		case "Endpoint", "Region":
			name = strings.ToLower(name)
		case "dry-run":
			name = "dryrun"
		}
		return pflag.NormalizedName(name)
	})
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	pkg.AddConnectionFlags(fs, &connection)
	if name != "presign" {
		fs.Var(&opts.partSize, "part-size", "Multipart transfer size (5MiB–5GiB)")
	}
	pkg.AddCobraFlags(flags, fs)
	if name == "presign" {
		cmd.Use = name + " [flags] s3://bucket/key"
		flags.IntVar(&expires, "expires-in", 3600, "GET URL lifetime in seconds (1–604800)")
	} else {
		flags.BoolVar(&opts.dryRun, "dryrun", false, "Print planned operations without changing any files or objects")
		flags.IntVarP(&opts.concurrency, "concurrency", "j", pkg.DefaultConcurrency, "Concurrent parts per object (1–128)")
		if name == "sync" {
			opts.recursive = true
			flags.BoolVar(&opts.deleteExtra, "delete", false, "Remove destination-only files after all copies succeed")
		} else {
			flags.BoolVarP(&opts.recursive, "recursive", "r", false, "Copy the contents of a directory or prefix")
			flags.BoolVar(&opts.force, "force", false, "Allow overwriting existing destinations")
		}
	}
	cmd.Args = func(c *cobra.Command, args []string) error {
		if name == "presign" {
			if err := cobra.ExactArgs(1)(c, args); err != nil {
				return err
			}
			var err error
			source.bucket, source.path, err = pkg.ParseS3URI(args[0])
			if err != nil {
				return err
			}
			if expires < 1 || expires > 604800 {
				return errors.New("--expires-in must be 1–604800 seconds")
			}
			return nil
		}
		if err := cobra.ExactArgs(2)(c, args); err != nil {
			return err
		}
		var err error
		if source, err = parseLocation(args[0]); err != nil {
			return err
		}
		if destination, err = parseLocation(args[1]); err != nil {
			return err
		}
		if !source.remote() && !destination.remote() {
			return errors.New("at least one path must use s3://")
		}
		if opts.concurrency < 1 || opts.concurrency > pkg.MaxConcurrency || opts.partSize.Value < 5<<20 || opts.partSize.Value > 5<<30 {
			return errors.New("concurrency must be 1–128 and part size must be 5MiB–5GiB")
		}
		if source.stream() || destination.stream() {
			if name != "cp" || opts.recursive {
				return errors.New("'-' is only supported for a single cp")
			}
			remote := source
			if source.stream() {
				remote = destination
			}
			if remote.isPrefix() {
				return errors.New("streaming requires an S3 object key")
			}
		}
		if !opts.recursive && source.isPrefix() {
			return errors.New("use --recursive for an S3 prefix")
		}
		return nil
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		connection.CredentialsExplicit = flags.Changed("ak") || flags.Changed("sk") || flags.Changed("session-token")
		connection.AWSDirectoryExplicit = flags.Changed("aws")
		connection.AWSProfileExplicit = flags.Changed("profile") || flags.Changed("p")
		cfg, err := pkg.ResolveConnectionConfig(connection, pkg.ConnectionConfig(connection))
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(c.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		client, err := pkg.NewS3Client(ctx, cfg)
		if err == nil {
			if name == "presign" {
				err = presign(ctx, client, source, expires, c.OutOrStdout())
			} else {
				client = pkg.WithS3IdleConnections(client, opts.concurrency, opts.concurrency)
				job := transfer{client: client, options: opts, output: c.OutOrStdout()}
				err = job.run(ctx, source, destination, c.InOrStdin())
			}
		}
		if err == nil {
			return nil
		}
		fmt.Fprintf(c.ErrOrStderr(), "s3%s: %v\n", name, err)
		if errors.Is(err, context.Canceled) {
			return pkg.ExitError(130)
		}
		return pkg.ExitError(1)
	}
	return cmd
}

func presign(ctx context.Context, client *s3.Client, source location, expires int, out io.Writer) error {
	signed, err := s3.NewPresignClient(client).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(source.bucket), Key: aws.String(source.path),
	}, s3.WithPresignExpires(time.Duration(expires)*time.Second))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, signed.URL)
	return err
}

func Run(name string, args []string, stdout, stderr io.Writer) int {
	cmd := NewCommand(name)
	cmd.Use, cmd.Aliases = "s3"+cmd.Use, nil
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs(args)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	err := cmd.Execute()
	if err == nil {
		return 0
	}
	var status pkg.ExitError
	if errors.As(err, &status) {
		return int(status)
	}
	fmt.Fprintln(stderr, cmd.Name()+":", err)
	return 2
}
