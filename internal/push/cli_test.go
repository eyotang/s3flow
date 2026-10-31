package push

import "testing"

func TestParseCLIReadsUploadAndConnectionFlags(t *testing.T) {
	opts, err := parseCLI([]string{"--profile", "production", "--aws", "./aws", "--ak", "key", "--sk", "secret", "--path-style=false", "--part-size", "64MiB", "--concurrency", "8", "--force", "./data", "s3://bucket/prefix/"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.source != "./data" || opts.destination != "s3://bucket/prefix/" {
		t.Fatalf("unexpected paths: %q, %q", opts.source, opts.destination)
	}
	if opts.partSize != 64<<20 || opts.concurrency != 8 || !opts.force {
		t.Fatal("upload flags were not preserved")
	}
	if opts.AWSProfile != "production" || opts.AWSDirectory != "./aws" || opts.PathStyle {
		t.Fatal("connection flags were not preserved")
	}
	if !opts.CredentialsExplicit || !opts.AWSProfileExplicit || !opts.AWSDirectoryExplicit {
		t.Fatal("explicit connection selection was lost")
	}
}
