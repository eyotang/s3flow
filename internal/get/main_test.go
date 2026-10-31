package get

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
				"用法:\n  s3flow get [参数] s3://bucket/key [输出文件]",
				"s3get [参数] 'https://Endpoint/bucket/key?预签名参数' [输出文件]",
				"参数（-flag 与 --flag 均可）:",
				"-aws DIR",
				"-Endpoint URL",
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
		"用法:\n  s3flow get [参数] s3://bucket/key [输出文件]",
		"参数（-flag 与 --flag 均可）:",
		"-aws DIR",
		"-Endpoint URL",
		"-profile NAME",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("expected stderr to contain %q, got:\n%s", want, stderr.String())
		}
	}
}
