package get

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIFlagDefaultsIncludeHelperText(t *testing.T) {
	var output bytes.Buffer
	opts := defaultCLIOptions()
	flags, _ := newCLIFlagSet(&opts, &output)

	flags.PrintDefaults()

	help := output.String()
	for _, want := range []string{
		"-ak VALUE\n",
		"AWS access key VALUE (or AWS_ACCESS_KEY_ID)",
		"-aws DIR\n",
		"AWS config directory DIR containing config and credentials (default: ~/.aws)",
		"-concurrency N\n",
		"Concurrent parts N (1-128) (default 16)",
		"-Endpoint URL\n",
		"S3 endpoint URL (e.g. https://s3.example.com)",
		"-o FILE\n",
		"Output file FILE",
		"-p NAME\n",
		"AWS named profile NAME (alias for --profile)",
		"-part-size SIZE\n",
		"Part size SIZE (e.g. 128MiB or 1GiB) (default 128.0 MiB)",
		"-path-style\n",
		"Use path-style S3 requests; false uses virtual-hosted style (bucket.Endpoint) (default true)",
		"-profile NAME\n",
		"AWS named profile NAME (default: AWS_PROFILE or default)",
		"-Region VALUE\n",
		"S3 region VALUE (fallback: cn-sh)",
		"-session-token VALUE\n",
		"AWS session token VALUE",
		"-sk VALUE\n",
		"AWS secret key VALUE (or AWS_SECRET_ACCESS_KEY)",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("expected PrintDefaults output to contain %q, got:\n%s", want, help)
		}
	}
	if strings.Contains(help, "-config ") {
		t.Errorf("expected obsolete YAML --config flag to be removed, got:\n%s", help)
	}
}

func TestDefaultPartSizeIs128MiB(t *testing.T) {
	opts := defaultCLIOptions()
	if opts.partSize != 128<<20 {
		t.Fatalf("default part size = %d, want %d", opts.partSize, 128<<20)
	}
}

func TestParseCLIReadsConnectionAndAWSFlags(t *testing.T) {
	AWSDirectory := filepath.Join(t.TempDir(), "aws")
	opts, err := parseCLI([]string{
		"--Endpoint", "https://cli.example.test/",
		"--ak", "cli-ak",
		"--sk", "cli-sk",
		"--session-token", "cli-token",
		"--Region", "cli-Region",
		"--aws", AWSDirectory,
		"--profile", "team",
		"--part-size", "4MiB",
		"--concurrency", "3",
		"--path-style",
		"--force",
		"s3://assets/releases/archive.tar",
		"archive.tar",
	})
	if err != nil {
		t.Fatalf("parse CLI: %v", err)
	}

	if opts.Endpoint != "https://cli.example.test/" {
		t.Fatalf("expected Endpoint flag, got %q", opts.Endpoint)
	}
	if opts.AccessKey != "cli-ak" || opts.SecretKey != "cli-sk" || opts.SessionToken != "cli-token" {
		t.Fatalf("expected CLI credentials, got ak=%q sk=%q token=%q", opts.AccessKey, opts.SecretKey, opts.SessionToken)
	}
	if opts.Region != "cli-Region" || opts.AWSDirectory != AWSDirectory || opts.AWSProfile != "team" {
		t.Fatalf("expected Region/AWS flags, got Region=%q directory=%q profile=%q", opts.Region, opts.AWSDirectory, opts.AWSProfile)
	}
	if !opts.CredentialsExplicit {
		t.Fatal("expected explicit CLI credentials to be recorded")
	}
	if !opts.AWSDirectoryExplicit || !opts.AWSProfileExplicit {
		t.Fatalf("expected explicit AWS selectors, directory=%v profile=%v", opts.AWSDirectoryExplicit, opts.AWSProfileExplicit)
	}
	if opts.source != "s3://assets/releases/archive.tar" || opts.output != "archive.tar" {
		t.Fatalf("unexpected source/output: %q %q", opts.source, opts.output)
	}
	if opts.partSize != 4<<20 || opts.concurrency != 3 {
		t.Fatalf("unexpected download settings: partSize=%d concurrency=%d", opts.partSize, opts.concurrency)
	}
	if !opts.PathStyle || !opts.force {
		t.Fatalf("expected path-style and force flags to be enabled")
	}

	short, err := parseCLI([]string{"-p", "short-profile", "s3://assets/object.bin"})
	if err != nil {
		t.Fatalf("parse -p alias: %v", err)
	}
	if short.AWSProfile != "short-profile" || short.AWSDirectory != "" || !short.AWSProfileExplicit || short.AWSDirectoryExplicit {
		t.Fatalf("-p selection = profile %q, directory %q; want profile %q with default directory", short.AWSProfile, short.AWSDirectory, "short-profile")
	}
}

func TestParseCLIAllowsDisablingDefaultPathStyle(t *testing.T) {
	opts, err := parseCLI([]string{"--path-style=false", "s3://assets/object.bin"})
	if err != nil {
		t.Fatalf("parse --path-style=false: %v", err)
	}
	if opts.PathStyle {
		t.Fatal("expected --path-style=false to select virtual-hosted-style")
	}
}
