package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/eyotang/s3flow/internal/update"
)

func TestNativeCommandsAndGroupedHelp(t *testing.T) {
	for _, name := range []string{"get", "push", "s3get", "s3push"} {
		var out, errOut bytes.Buffer
		if code := run([]string{name, "--help"}, &out, &errOut); code != 0 || errOut.Len() != 0 {
			t.Fatalf("%s: code=%d stderr=%s", name, code, &errOut)
		}
		for _, heading := range []string{"连接配置:", "身份凭据:", "传输设置:", "文件操作:", "通用参数:"} {
			if !strings.Contains(out.String(), heading) {
				t.Fatalf("%s: missing %s", name, heading)
			}
		}
		if !strings.Contains(out.String(), "--concurrency N") {
			t.Fatal(out.String())
		}
	}
}

func TestNativeVersionsDoNotStartTransfers(t *testing.T) {
	previous := update.Version
	update.Version = "v1.2.3"
	t.Cleanup(func() { update.Version = previous })
	for _, args := range [][]string{{"--version"}, {"get", "--version"}, {"push", "--version"}} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 0 || errOut.Len() != 0 {
			t.Fatalf("%v: code=%d stderr=%s", args, code, &errOut)
		}
		if !strings.HasSuffix(out.String(), "v1.2.3\n") {
			t.Fatal(out.String())
		}
	}
}

func TestCobraRejectsBadArgumentsAndParsesFlagsAfterPaths(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"unknown"}, "unknown command"},
		{[]string{"get", "--unknown"}, "unknown flag"},
		{[]string{"get", "s3://bucket/key", "--concurrency", "129"}, "--concurrency 必须"},
		{[]string{"get", "-p", "example", "-r", "s3://bucket/", "--part-size", "0"}, "part-size"},
		{[]string{"push", "file", "s3://bucket/key", "--part-size", "1MiB"}, "上传分片大小"},
		{[]string{"push", t.TempDir(), "s3://bucket/prefix/", "-p", "example", "--force"}, "没有可上传的文件"},
	} {
		var out, errOut bytes.Buffer
		code := run(tc.args, &out, &errOut)
		if code != 2 || !strings.Contains(errOut.String(), tc.want) || out.Len() != 0 {
			t.Fatalf("%v: code=%d stdout=%s stderr=%s", tc.args, code, &out, &errOut)
		}
	}
}
