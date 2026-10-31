package push

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/eyotang/s3flow/internal/pkg"
)

type cliOptions struct {
	pkg.Options
	version     bool
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
	fs.BoolVar(&opts.version, "version", false, "Print version and exit")
	fs.IntVar(&opts.concurrency, "concurrency", pkg.DefaultConcurrency, "Concurrent parts `N` (1-128)")
	fs.BoolVar(&opts.force, "force", false, "Allow a non-empty destination prefix and overwrite matching objects")
	partSize := &pkg.ByteSizeFlag{Value: pkg.DefaultPartSize}
	fs.Var(partSize, "part-size", "Part size `SIZE` (e.g. 128MiB or 1GiB)")
	fs.Usage = func() {
		fmt.Fprintln(output, "Usage:")
		fmt.Fprintln(output, "  s3flow push [flags] <local-path> s3://bucket/key-or-prefix/")
		fmt.Fprintln(output, "  s3push [flags] <local-path> s3://bucket/key-or-prefix/")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "Flags (both -flag and --flag are accepted):")
		pkg.PrintGroupedFlags(output, fs)
	}
	return fs, partSize
}

func parseCLI(args []string) (cliOptions, error) {
	opts := defaultCLIOptions()
	flags, size := newCLIFlagSet(&opts, io.Discard)
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if opts.version {
		return opts, nil
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
	return validateCLI(opts, size.Value, flags.Args())
}

func validateCLI(opts cliOptions, size int64, args []string) (cliOptions, error) {
	opts.partSize = size
	if len(args) != 2 || opts.concurrency < 1 || opts.concurrency > pkg.MaxConcurrency || opts.partSize < 5<<20 || opts.partSize > 5<<30 {
		return cliOptions{}, errors.New("A local path and an S3 destination are required; concurrency must be 1-128 and upload part size must be 5MiB-5GiB")
	}
	opts.source, opts.destination = args[0], args[1]
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
