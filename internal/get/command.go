package get

import (
	"fmt"
	"io"

	"github.com/eyotang/s3flow/internal/pkg"
	"github.com/eyotang/s3flow/internal/update"
	"github.com/spf13/cobra"
)

// NewCommand binds Cobra flags directly to the shared transfer implementation.
func NewCommand() *cobra.Command {
	opts := defaultCLIOptions()
	fs, size := newCLIFlagSet(&opts, io.Discard)
	cmd := &cobra.Command{Use: "get [flags] <s3://bucket/key|presigned-url> [output]", Aliases: []string{"s3get"}, Short: "Download S3 files or directories", Version: update.Version}
	pkg.AddCobraFlags(cmd.Flags(), fs)
	cmd.InitDefaultHelpFlag()
	cmd.Flags().Lookup("help").Usage = "Show help and exit"
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		output := c.OutOrStdout()
		fmt.Fprintln(output, "Usage:")
		fmt.Fprintln(output, "  s3flow get [flags] <s3://bucket/key|presigned-url> [output]")
		fmt.Fprintln(output, "\nFlags (long: --name; short: -p):")
		pkg.PrintGroupedCobraFlags(output, c.Flags())
	})
	cmd.Args = func(c *cobra.Command, args []string) error {
		opts.CredentialsExplicit = c.Flags().Changed("ak") || c.Flags().Changed("sk") || c.Flags().Changed("session-token")
		opts.AWSDirectoryExplicit = c.Flags().Changed("aws")
		opts.AWSProfileExplicit = c.Flags().Changed("p") || c.Flags().Changed("profile")
		resolved, err := validateCLI(opts, size.Value, args)
		if err != nil {
			return err
		}
		opts = resolved
		return nil
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		code := runTransfer(opts, c.OutOrStdout(), c.ErrOrStderr())
		if code != 0 {
			return pkg.ExitError(code)
		}
		return nil
	}
	return cmd
}
