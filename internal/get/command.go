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
	cmd := &cobra.Command{Use: "get [参数] <s3://bucket/key|预签名URL> [输出文件]", Aliases: []string{"s3get"}, Short: "下载 S3 文件或目录", Version: update.Version}
	pkg.AddCobraFlags(cmd.Flags(), fs)
	cmd.InitDefaultHelpFlag()
	cmd.Flags().Lookup("help").Usage = "显示帮助并退出"
	cmd.SetVersionTemplate("{{.CommandPath}} {{.Version}}\n")
	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		output := c.OutOrStdout()
		fmt.Fprintln(output, "用法:")
		fmt.Fprintln(output, "  s3flow get [参数] <s3://bucket/key|预签名URL> [输出文件]")
		fmt.Fprintln(output, "\n参数（-p、-o、-r 等短参数与 --flag 长参数）:")
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
