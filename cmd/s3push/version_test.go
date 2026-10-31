package main

import (
	"bytes"
	"testing"

	"github.com/eyotang/s3flow/internal/update"
)

func TestVersion(t *testing.T) {
	previous := update.Version
	update.Version = "v1.2.3"
	t.Cleanup(func() { update.Version = previous })
	for _, flag := range []string{"--version", "-version"} {
		t.Run(flag, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{flag}, &stdout, &stderr)
			if code != 0 || stdout.String() != "s3push v1.2.3\n" || stderr.Len() != 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}
