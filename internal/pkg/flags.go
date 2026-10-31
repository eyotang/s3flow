package pkg

import (
	"flag"
	"fmt"
	"io"

	"github.com/spf13/pflag"
)

func AddConnectionFlags(fs *flag.FlagSet, opts *Options) {
	fs.StringVar(&opts.Endpoint, "Endpoint", "", "S3 endpoint `URL` (e.g. https://s3.example.com)")
	fs.StringVar(&opts.AccessKey, "ak", "", "AWS access key `VALUE` (or AWS_ACCESS_KEY_ID)")
	fs.StringVar(&opts.SecretKey, "sk", "", "AWS secret key `VALUE` (or AWS_SECRET_ACCESS_KEY)")
	fs.StringVar(&opts.SessionToken, "session-token", "", "AWS session token `VALUE` (or AWS_SESSION_TOKEN)")
	fs.StringVar(&opts.Region, "Region", "", "S3 region `VALUE` (fallback: cn-sh)")
	fs.StringVar(&opts.AWSDirectory, "aws", "", "AWS config directory `DIR` containing config and credentials (default: ~/.aws)")
	fs.StringVar(&opts.AWSProfile, "p", "", "AWS named profile `NAME` (alias for --profile)")
	fs.StringVar(&opts.AWSProfile, "profile", "", "AWS named profile `NAME` (default: AWS_PROFILE or default)")
	fs.BoolVar(&opts.PathStyle, "path-style", true, "Use path-style S3 requests; false uses virtual-hosted style (bucket.Endpoint)")
}

// PrintGroupedFlags keeps the registered flag definitions and defaults as the
// source of truth, while placing related options together in help output.
func PrintGroupedFlags(output io.Writer, fs *flag.FlagSet) {
	titles := []string{"Connection", "Credentials", "Transfer", "Files", "General"}
	groups := make([]*flag.FlagSet, len(titles))
	for i := range groups {
		groups[i] = flag.NewFlagSet("", flag.ContinueOnError)
		groups[i].SetOutput(output)
	}
	fs.VisitAll(func(f *flag.Flag) {
		index := flagGroup(f.Name)
		groups[index].Var(f.Value, f.Name, f.Usage)
		groups[index].Lookup(f.Name).DefValue = f.DefValue
	})
	for i, group := range groups {
		fmt.Fprintf(output, "\n%s:\n", titles[i])
		group.PrintDefaults()
		if i == 4 {
			fmt.Fprintln(output, "  -h, --help\n    \tShow help and exit")
		}
	}
}

// ExitError carries an already-reported transfer exit status to the entrypoint.
type ExitError int

func (e ExitError) Error() string { return fmt.Sprintf("Command exited with status %d", int(e)) }

func flagGroup(name string) int {
	switch name {
	case "Endpoint", "Region", "aws", "p", "profile", "path-style":
		return 0
	case "ak", "sk", "session-token":
		return 1
	case "concurrency", "part-size":
		return 2
	case "o", "output", "r", "recursive", "force":
		return 3
	default:
		return 4
	}
}

// PrintGroupedCobraFlags uses pflag's native long and short flag syntax.
func PrintGroupedCobraFlags(output io.Writer, flags *pflag.FlagSet) {
	titles := []string{"Connection", "Credentials", "Transfer", "Files", "General"}
	groups := make([]*pflag.FlagSet, len(titles))
	for i := range groups {
		groups[i] = pflag.NewFlagSet("", pflag.ContinueOnError)
	}
	flags.VisitAll(func(f *pflag.Flag) { groups[flagGroup(f.Name)].AddFlag(f) })
	for i, group := range groups {
		fmt.Fprintf(output, "\n%s:\n%s", titles[i], group.FlagUsages())
	}
}

// AddCobraFlags reuses definitions while presenting canonical long/short pairs.
func AddCobraFlags(target *pflag.FlagSet, source *flag.FlagSet) {
	source.VisitAll(func(f *flag.Flag) {
		converted := pflag.PFlagFromGoFlag(f)
		switch f.Name {
		case "profile":
			converted.Shorthand = "p"
		case "output":
			converted.Shorthand = "o"
		case "recursive":
			converted.Shorthand = "r"
		case "p", "o", "r":
			converted.Shorthand = ""
			converted.Hidden = true
		}
		target.AddFlag(converted)
	})
}
