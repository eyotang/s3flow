package rg

import (
	"fmt"
	"regexp"
	"strings"
)

type objectGlob struct {
	pattern  *regexp.Regexp
	baseName bool
	exclude  bool
}

func compileGlobs(patterns []string) ([]objectGlob, error) {
	var globs []objectGlob
	for _, raw := range patterns {
		pattern, exclude := strings.CutPrefix(raw, "!")
		baseName := !strings.Contains(pattern, "/")
		parser := globParser{pattern: []rune(strings.TrimPrefix(pattern, "/")), path: !baseName}
		expression, err := parser.branch(0)
		if err != nil {
			return nil, fmt.Errorf("invalid glob %q: %w", raw, err)
		}
		if expression == globRecursivePrefix {
			expression = ".*"
		}
		compiled, err := regexp.Compile(`(?s)\A(?:` + expression + `)\z`)
		if err != nil {
			return nil, fmt.Errorf("invalid glob %q: %w", raw, err)
		}
		globs = append(globs, objectGlob{compiled, baseName, exclude})
	}
	return globs, nil
}

func accepts(globs []objectGlob, relative string) bool {
	if len(globs) == 0 {
		return true
	}
	// globset matches bytes, including for '?' and character classes.
	name := globBytes(relative)
	base := name[strings.LastIndexByte(name, '/')+1:]
	includes, matched := false, false
	for _, glob := range globs {
		value := name
		if glob.baseName {
			value = base
		}
		match := glob.pattern.MatchString(value)
		if glob.exclude {
			if match {
				return false
			}
		} else {
			includes = true
			matched = matched || match
		}
	}
	return !includes || matched
}

func globBytes(value string) string {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x80 {
			continue
		}
		var encoded strings.Builder
		encoded.Grow(len(value) + 1)
		encoded.WriteString(value[:i])
		for j := i; j < len(value); j++ {
			encoded.WriteRune(rune(value[j]))
		}
		return encoded.String()
	}
	return value
}

const (
	globRecursivePrefix = `(?:/?|.*/)`
	globRecursiveMiddle = `(?:/|/.*/)`
	globRecursiveSuffix = `/.*`
)

// A slash-delimited ** consumes whole path components. Other stars are
// confined to a component in path patterns. Braces can group alternatives.
type globParser struct {
	pattern []rune
	pos     int
	path    bool
}

func (p *globParser) branch(depth int) (string, error) {
	if depth > 250 {
		return "", fmt.Errorf("too many nested alternatives")
	}
	var parts []string
	for p.pos < len(p.pattern) {
		char := p.pattern[p.pos]
		if depth > 0 && (char == ',' || char == '}') {
			break
		}
		p.pos++
		switch char {
		case '}':
			return "", fmt.Errorf("unopened alternation")
		case '{':
			var alternatives []string
			for {
				alternative, err := p.branch(depth + 1)
				if err != nil {
					return "", err
				}
				if alternative != "" {
					alternatives = append(alternatives, alternative)
				}
				if p.pos == len(p.pattern) {
					return "", fmt.Errorf("unclosed alternation")
				}
				p.pos++
				if p.pattern[p.pos-1] == '}' {
					break
				}
			}
			if len(alternatives) > 0 {
				parts = append(parts, "(?:"+strings.Join(alternatives, "|")+")")
			}
		case '*':
			parts = p.star(parts, depth > 0)
		case '?':
			if p.path {
				parts = append(parts, `[^/]`)
			} else {
				parts = append(parts, `.`)
			}
		case '[':
			class, err := p.class()
			if err != nil {
				return "", err
			}
			parts = append(parts, class)
		case '\\':
			if p.pos == len(p.pattern) {
				return "", fmt.Errorf("dangling escape")
			}
			parts = append(parts, regexp.QuoteMeta(globBytes(string(p.pattern[p.pos]))))
			p.pos++
		default:
			parts = append(parts, regexp.QuoteMeta(globBytes(string(char))))
		}
	}
	return strings.Join(parts, ""), nil
}

func (p *globParser) star(parts []string, inGroup bool) []string {
	single := `.*`
	if p.path {
		single = `[^/]*`
	}
	if p.pos == len(p.pattern) || p.pattern[p.pos] != '*' {
		return append(parts, single)
	}
	start := p.pos - 1
	p.pos++
	end := p.pos == len(p.pattern)
	slash := !end && p.pattern[p.pos] == '/'
	if len(parts) == 0 && (end || slash) {
		if slash {
			p.pos++
		}
		return append(parts, globRecursivePrefix)
	}
	groupEnd := !end && inGroup && (p.pattern[p.pos] == ',' || p.pattern[p.pos] == '}')
	if len(parts) == 0 || start == 0 || p.pattern[start-1] != '/' || !(end || slash || groupEnd) {
		return append(parts, single)
	}
	last := len(parts) - 1
	if slash {
		p.pos++
	}
	if parts[last] == globRecursivePrefix || parts[last] == globRecursiveSuffix {
		return parts
	}
	parts[last] = globRecursiveSuffix
	if slash {
		parts[last] = globRecursiveMiddle
	}
	return parts
}

func (p *globParser) class() (string, error) {
	var class strings.Builder
	class.WriteByte('[')
	if p.pos < len(p.pattern) && (p.pattern[p.pos] == '!' || p.pattern[p.pos] == '^') {
		class.WriteByte('^')
		p.pos++
	}
	type characterRange struct{ first, last rune }
	var ranges []characterRange
	wantEnd := false
	for p.pos < len(p.pattern) {
		char := p.pattern[p.pos]
		p.pos++
		if char == ']' && len(ranges) > 0 {
			if wantEnd {
				ranges = append(ranges, characterRange{'-', '-'})
			}
			for _, span := range ranges {
				for _, b := range []byte(string(span.first)) {
					fmt.Fprintf(&class, `\x%02x`, b)
				}
				if span.first != span.last {
					class.WriteByte('-')
					for _, b := range []byte(string(span.last)) {
						fmt.Fprintf(&class, `\x%02x`, b)
					}
				}
			}
			class.WriteByte(']')
			return class.String(), nil
		}
		if wantEnd {
			span := &ranges[len(ranges)-1]
			if char < span.first {
				return "", fmt.Errorf("invalid character range %q-%q", span.first, char)
			}
			span.last = char
			wantEnd = false
		} else if char == '-' && len(ranges) > 0 {
			wantEnd = true
		} else {
			ranges = append(ranges, characterRange{char, char})
		}
	}
	return "", fmt.Errorf("unclosed character class")
}
