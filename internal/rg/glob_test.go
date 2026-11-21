package rg

import "testing"

func TestObjectGlobs(t *testing.T) {
	for _, tc := range []struct {
		patterns []string
		key      string
		want     bool
	}{
		{[]string{"*.log"}, "logs/app/deep/a.log", true},
		{[]string{"logs/*.log"}, "logs/sub/a.log", false},
		{[]string{"logs/**/*.log"}, "logs/a.log", true},
		{[]string{"logs/**/*.log"}, "logs/app/deep/a.log", true},
		{[]string{"**/sub/*"}, "sub/a.log", true},
		{[]string{"**/sub/*"}, "logs/app/sub/a.log", true},
		{[]string{"/sub/*"}, "sub/a.log", true},
		{[]string{"/sub/*"}, "logs/sub/a.log", false},
		{[]string{"logs/**"}, "logs", false},
		{[]string{"logs/**"}, "logs/app/a.log", true},
		{[]string{"a**/x"}, "a/extra/x", false},
		{[]string{"!**/old/**", "**/*.log"}, "logs/old/a.log", false},
		{[]string{"**/{a,b}.[lt]og"}, "logs/deep/b.log", true},
		{[]string{"[!a].log"}, "logs/b.log", true},
		{[]string{"[a-b-c].log"}, "logs/-.log", false},
		{[]string{"[a-b-c].log"}, "logs/c.log", true},
		{[]string{`\[a\].log`}, "logs/[a].log", true},
		{[]string{"*.log"}, "logs/multi\nline.log", true},
		{[]string{"?.log"}, "logs/é.log", false},
		{[]string{"??.log"}, "logs/é.log", true},
		{[]string{"é.log"}, "logs/é.log", true},
	} {
		globs, err := compileGlobs(tc.patterns)
		if err != nil {
			t.Fatalf("%q: %v", tc.patterns, err)
		}
		if got := accepts(globs, tc.key); got != tc.want {
			t.Errorf("%q on %q: got %t, want %t", tc.patterns, tc.key, got, tc.want)
		}
	}
	for _, pattern := range []string{"[", "[z-a]", "{a,b", `foo\`} {
		if _, err := compileGlobs([]string{pattern}); err == nil {
			t.Errorf("invalid pattern %q accepted", pattern)
		}
	}
}
