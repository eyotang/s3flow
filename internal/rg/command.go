package rg

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"regexp"
	"regexp/syntax"
	"runtime"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
)

type target struct{ bucket, key string }

type options struct {
	pkg.Options
	patterns, globs []string
	matchers        []func([]byte) bool
	targets         []target
	jobs            int

	ignoreCase, literal, invert  bool
	numbers, count, files, quiet bool
}

func NewCommand() *cobra.Command {
	opts := options{Options: pkg.Options{PathStyle: true}}
	fs := flag.NewFlagSet("s3rg", flag.ContinueOnError)
	pkg.AddConnectionFlags(fs, &opts.Options)
	cmd := &cobra.Command{Use: "rg [flags] PATTERN s3://bucket/path ...", Aliases: []string{"s3rg"}, Short: "Search S3 object contents", Version: update.Version}
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	flags := cmd.Flags()
	pkg.AddCobraFlags(flags, fs)
	flags.StringArrayVarP(&opts.patterns, "regexp", "e", nil, "Add a search pattern")
	flags.StringArrayVarP(&opts.globs, "glob", "g", nil, "Filter listed objects by glob")
	flags.IntVarP(&opts.jobs, "concurrency", "j", min(runtime.GOMAXPROCS(0), pkg.MaxConcurrency), "Concurrent object reads (1–128)")
	flags.BoolVarP(&opts.ignoreCase, "ignore-case", "i", false, "Ignore case")
	flags.BoolVarP(&opts.literal, "fixed-strings", "F", false, "Match literal strings")
	flags.BoolVarP(&opts.invert, "invert-match", "v", false, "Select nonmatching lines")
	flags.BoolVarP(&opts.numbers, "line-number", "n", false, "Show line numbers")
	flags.BoolVarP(&opts.count, "count", "c", false, "Count matching lines per object")
	flags.BoolVarP(&opts.files, "files-with-matches", "l", false, "Show matching object URIs")
	flags.BoolVarP(&opts.quiet, "quiet", "q", false, "Stop at the first match without printing results")
	cmd.MarkFlagsMutuallyExclusive("count", "files-with-matches", "quiet")
	cmd.Args = func(c *cobra.Command, args []string) error {
		if len(opts.patterns) == 0 && len(args) > 0 {
			opts.patterns, args = args[:1], args[1:]
		}
		if len(args) == 0 {
			return errors.New("a search pattern and at least one S3 target are required")
		}
		if opts.jobs < 1 || opts.jobs > pkg.MaxConcurrency {
			return errors.New("concurrency must be 1–128")
		}
		if err := opts.compileMatchers(); err != nil {
			return err
		}
		for _, glob := range opts.globs {
			pattern := strings.TrimPrefix(glob, "!")
			if _, err := path.Match(pattern, ""); pattern == "" || err != nil {
				return fmt.Errorf("invalid glob %q", glob)
			}
		}
		for _, arg := range args {
			bucket, key, err := pkg.ParseS3Location(arg)
			if err != nil {
				return err
			}
			opts.targets = append(opts.targets, target{bucket, key})
		}
		opts.CredentialsExplicit = flags.Changed("ak") || flags.Changed("sk") || flags.Changed("session-token")
		opts.AWSDirectoryExplicit = flags.Changed("aws")
		opts.AWSProfileExplicit = flags.Changed("p") || flags.Changed("profile")
		return nil
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(c.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		cfg, err := pkg.ResolveConnectionConfig(opts.Options, pkg.ConnectionConfig(opts.Options))
		if err == nil {
			client, clientErr := pkg.NewS3Client(ctx, cfg)
			if clientErr == nil {
				// Discovery can issue a request while all object workers are busy.
				connections := opts.jobs + 1
				client = pkg.WithS3IdleConnections(client, connections, connections)
				return search(ctx, client, opts, c.OutOrStdout(), c.ErrOrStderr())
			}
			err = clientErr
		}
		fmt.Fprintln(c.ErrOrStderr(), "s3rg:", err)
		return pkg.ExitError(2)
	}
	return cmd
}

func (opts *options) compileMatchers() error {
	for _, pattern := range opts.patterns {
		flags := syntax.Perl
		if opts.literal {
			flags |= syntax.Literal
		}
		if opts.ignoreCase {
			flags |= syntax.FoldCase
		}
		parsed, err := syntax.Parse(pattern, flags)
		if err != nil {
			return err
		}
		if match := literalMatcher(parsed); match != nil {
			opts.matchers = append(opts.matchers, match)
			continue
		}
		if opts.literal {
			pattern = regexp.QuoteMeta(pattern)
		}
		if opts.ignoreCase {
			pattern = "(?i)" + pattern
		}
		expression, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		opts.matchers = append(opts.matchers, expression.Match)
	}
	return nil
}

// literalMatcher handles expressions whose match is exactly a byte search,
// optionally anchored to the beginning of the line.
func literalMatcher(expr *syntax.Regexp) func([]byte) bool {
	anchored := expr.Op == syntax.OpConcat && len(expr.Sub) == 2 &&
		(expr.Sub[0].Op == syntax.OpBeginText || expr.Sub[0].Op == syntax.OpBeginLine)
	if anchored {
		expr = expr.Sub[1]
	}
	if expr.Op == syntax.OpEmptyMatch {
		return func([]byte) bool { return true }
	}
	if expr.Op != syntax.OpLiteral || expr.Flags&syntax.FoldCase != 0 {
		return nil
	}
	literal := string(expr.Rune)
	// regexp also matches U+FFFD against malformed UTF-8 in the input.
	if strings.ContainsRune(literal, utf8.RuneError) {
		return nil
	}
	needle := []byte(literal)
	if anchored {
		return func(line []byte) bool { return bytes.HasPrefix(line, needle) }
	}
	return func(line []byte) bool { return bytes.Contains(line, needle) }
}

func Run(args []string, stdout, stderr io.Writer) int {
	cmd := NewCommand()
	cmd.Use = "s3" + cmd.Use
	cmd.Aliases = nil
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
	fmt.Fprintln(stderr, "s3rg:", err)
	return 2
}
