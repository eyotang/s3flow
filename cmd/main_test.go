package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunHelpPrintsUsageAndExitsSuccessfully(t *testing.T) {
	for _, helpFlag := range []string{"-h", "--help"} {
		t.Run(helpFlag, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			exitCode := run([]string{helpFlag}, &stdout, &stderr)

			if exitCode != 0 {
				t.Fatalf("expected help to exit successfully, got %d", exitCode)
			}
			if stderr.Len() != 0 {
				t.Fatalf("expected help not to write stderr, got:\n%s", stderr.String())
			}
			for _, want := range []string{
				"用法:\n  s3get [参数] s3://bucket/key [输出文件]",
				"s3get [参数] 'https://endpoint/bucket/key?预签名参数' [输出文件]",
				"参数（-flag 与 --flag 均可）:",
				"-aws DIR",
				"-endpoint URL",
				"-p NAME",
				"-part-size SIZE",
				"-profile NAME",
			} {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("expected help output to contain %q, got:\n%s", want, stdout.String())
				}
			}
		})
	}
}

func TestCLIFlagDefaultsIncludeHelperText(t *testing.T) {
	var output bytes.Buffer
	opts := defaultCLIOptions()
	flags, _ := newCLIFlagSet(&opts, &output)

	flags.PrintDefaults()

	help := output.String()
	for _, want := range []string{
		"-ak VALUE\n",
		"access key VALUE，也可使用 AWS_ACCESS_KEY_ID",
		"-aws DIR\n",
		"AWS 配置目录 DIR，目录内包含 config 与 credentials（默认 ~/.aws）",
		"-concurrency N\n",
		"并发分片数 N，范围 1-128 (default 16)",
		"-endpoint URL\n",
		"S3 endpoint URL，例如 https://s3.example.com",
		"-o FILE\n",
		"输出文件 FILE",
		"-p NAME\n",
		"AWS 命名 profile NAME（等同 --profile）",
		"-part-size SIZE\n",
		"分片大小 SIZE，例如 128MiB、1GiB (default 128.0 MiB)",
		"-path-style\n",
		"使用 path-style S3 请求；设为 false 时使用 bucket.endpoint (default true)",
		"-profile NAME\n",
		"AWS 命名 profile NAME（语义与 aws --profile 相同，默认 AWS_PROFILE/default）",
		"-region VALUE\n",
		"S3 region VALUE（未配置时使用 cn-sh）",
		"-session-token VALUE\n",
		"临时凭据 session token VALUE",
		"-sk VALUE\n",
		"secret key VALUE，也可使用 AWS_SECRET_ACCESS_KEY",
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

func TestRunInvalidFlagPrintsErrorAndUsageToStderr(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"--does-not-exist"}, &stdout, &stderr)

	if exitCode != 2 {
		t.Fatalf("expected invalid arguments to exit with code 2, got %d", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected invalid arguments not to write stdout, got:\n%s", stdout.String())
	}
	for _, want := range []string{
		"参数错误: flag provided but not defined: -does-not-exist",
		"用法:\n  s3get [参数] s3://bucket/key [输出文件]",
		"参数（-flag 与 --flag 均可）:",
		"-aws DIR",
		"-endpoint URL",
		"-profile NAME",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("expected stderr to contain %q, got:\n%s", want, stderr.String())
		}
	}
}
