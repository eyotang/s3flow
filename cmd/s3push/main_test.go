package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPushHelpAndInvalidFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{"--output", "x"}, 2},
		{[]string{"--part-size", "1MiB", "x", "s3://bucket/key"}, 2},
	} {
		var out, errOut bytes.Buffer
		if code := run(tc.args, &out, &errOut); code != tc.code {
			t.Fatalf("%v: %d", tc.args, code)
		}
		if tc.code == 0 && (!bytes.Contains(out.Bytes(), []byte("s3push")) || bytes.Contains(out.Bytes(), []byte("-output"))) {
			t.Fatal(out.String())
		}
	}
}

func TestEmptyDirectoryFailsBeforeAWSConfiguration(t *testing.T) {
	for _, nested := range []bool{false, true} {
		dir := t.TempDir()
		if nested {
			if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0700); err != nil {
				t.Fatal(err)
			}
		}
		for _, force := range []bool{false, true} {
			// Invalid credentials would cause a configuration error if reached.
			args := []string{"--ak", "only-key"}
			if force {
				args = append(args, "--force")
			}
			args = append(args, dir, "s3://bucket/prefix/")
			var out, errOut bytes.Buffer
			if code := run(args, &out, &errOut); code != 2 {
				t.Fatalf("exit=%d", code)
			}
			if !strings.Contains(errOut.String(), "没有可上传的文件") || out.Len() != 0 {
				t.Fatalf("stdout=%q stderr=%q", out.String(), errOut.String())
			}
		}
	}
}
