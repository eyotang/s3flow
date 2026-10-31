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
				"Usage:\n  s3flow get [flags] s3://bucket/key [output]",
				"s3get [flags] 'https://Endpoint/bucket/key?presigned-parameters' [output]",
				"Flags (both -flag and --flag are accepted):",
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
		"Argument error: flag provided but not defined: -does-not-exist",
		"Usage:\n  s3flow get [flags] s3://bucket/key [output]",
		"Flags (both -flag and --flag are accepted):",
		"-aws DIR",
		"-Endpoint URL",
		"-profile NAME",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("expected stderr to contain %q, got:\n%s", want, stderr.String())
		}
	}
}
