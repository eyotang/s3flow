package pkg

import (
	"bytes"
	"flag"
	"regexp"
	"strings"
	"testing"
)

func TestGroupedHelpPreservesFlagsAndDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var opts Options
	AddConnectionFlags(fs, &opts)
	fs.Int("concurrency", 16, "并发分片数 `N`")
	fs.Bool("force", false, "覆盖文件")
	fs.Bool("version", false, "显示版本")
	fs.String("future-option", "default", "新参数")
	// Printing help after parsing must still show the declared defaults.
	if err := fs.Parse([]string{"--concurrency", "8"}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	PrintGroupedFlags(&output, fs)
	help := output.String()
	headings := []string{"连接配置:", "身份凭据:", "传输设置:", "文件操作:", "通用参数:"}
	last := -1
	for _, heading := range headings {
		position := strings.Index(help, heading)
		if position <= last {
			t.Fatalf("missing or unordered %q: %s", heading, help)
		}
		last = position
	}
	fs.VisitAll(func(f *flag.Flag) {
		pattern := regexp.MustCompile(`(?m)^  -` + regexp.QuoteMeta(f.Name) + `(?: |$)`)
		if len(pattern.FindAllString(help, -1)) != 1 {
			t.Fatalf("flag %s missing or repeated: %s", f.Name, help)
		}
	})
	for _, want := range []string{"(default 16)", "(default true)", "-h, --help"} {
		if !strings.Contains(help, want) {
			t.Fatalf("missing %q: %s", want, help)
		}
	}
	if strings.Contains(help, "(default 8)") {
		t.Fatal("parsed value replaced original default")
	}
}
