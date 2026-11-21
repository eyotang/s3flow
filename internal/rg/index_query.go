package rg

import (
	"regexp/syntax"
	"slices"
	"unicode"
	"unicode/utf8"
)

// A nil condition accepts every object. All other conditions describe a
// necessary property, so falling back to nil only costs extra object reads.
type gramCondition struct {
	gram     uint32
	all      bool
	children []*gramCondition
}

func combineGrams(all bool, conditions []*gramCondition) *gramCondition {
	children := make([]*gramCondition, 0, len(conditions))
	for _, condition := range conditions {
		if condition != nil {
			children = append(children, condition)
		} else if !all {
			return nil
		}
	}
	switch len(children) {
	case 0:
		return nil
	case 1:
		return children[0]
	default:
		return &gramCondition{all: all, children: children}
	}
}

func planGrams(expr *syntax.Regexp) *gramCondition {
	switch expr.Op {
	case syntax.OpLiteral:
		return literalGrams(expr.Rune, expr.Flags&syntax.FoldCase != 0)
	case syntax.OpCapture, syntax.OpPlus:
		return planGrams(expr.Sub[0])
	case syntax.OpRepeat:
		if expr.Min > 0 {
			return planGrams(expr.Sub[0])
		}
	case syntax.OpConcat, syntax.OpAlternate:
		conditions := make([]*gramCondition, 0, len(expr.Sub))
		for _, part := range expr.Sub {
			conditions = append(conditions, planGrams(part))
		}
		if expr.Op == syntax.OpConcat {
			conditions = append(conditions, concatGrams(expr.Sub)...)
		}
		return combineGrams(expr.Op == syntax.OpConcat, conditions)
	}
	return nil
}

// Join bounded literal runs across syntax nodes. Go may factor an expression
// such as foo|fob into a short literal followed by a character class.
func concatGrams(parts []*syntax.Regexp) []*gramCondition {
	var conditions []*gramCondition
	var run []string
	var joined int
	for _, part := range parts {
		values := shortSpellings(part)
		if len(run) > 0 && len(values) > 0 && joined < 8 && len(run)*len(values) <= 32 {
			var next []string
			for _, before := range run {
				for _, after := range values {
					next = append(next, before+after)
				}
			}
			run = next
			joined++
			continue
		}
		if joined > 1 {
			conditions = append(conditions, spellingGrams(run))
		}
		run, joined = values, 1
	}
	if joined > 1 {
		conditions = append(conditions, spellingGrams(run))
	}
	return conditions
}

func shortSpellings(expr *syntax.Regexp) []string {
	switch expr.Op {
	case syntax.OpLiteral:
		if expr.Flags&syntax.FoldCase == 0 && len(expr.Rune) <= 64 && !slices.Contains(expr.Rune, utf8.RuneError) {
			return []string{string(expr.Rune)}
		}
	case syntax.OpCapture:
		return shortSpellings(expr.Sub[0])
	case syntax.OpCharClass:
		var values []string
		for i := 0; i < len(expr.Rune); i += 2 {
			first, last := expr.Rune[i], expr.Rune[i+1]
			if int(last-first)+1+len(values) > 8 || (first <= utf8.RuneError && last >= utf8.RuneError) {
				return nil
			}
			for r := first; r <= last; r++ {
				values = append(values, string(r))
			}
		}
		return values
	}
	return nil
}

func spellingGrams(spellings []string) *gramCondition {
	conditions := make([]*gramCondition, 0, len(spellings))
	for _, spelling := range spellings {
		conditions = append(conditions, byteGrams(spelling))
	}
	return combineGrams(false, conditions)
}

func gramCode(a, b, c byte) uint32 {
	return uint32(a)<<16 | uint32(b)<<8 | uint32(c)
}

func byteGrams(value string) *gramCondition {
	var terms []*gramCondition
	for i := 2; i < len(value); i++ {
		terms = append(terms, &gramCondition{gram: gramCode(value[i-2], value[i-1], value[i])})
	}
	return combineGrams(true, terms)
}

func literalGrams(literal []rune, fold bool) *gramCondition {
	// regexp can also match U+FFFD against an invalid byte. Grams involving
	// that rune's UTF-8 encoding would not be required in that case.
	if len(literal) > 4096 || slices.Contains(literal, utf8.RuneError) {
		return nil
	}
	if !fold {
		return byteGrams(string(literal))
	}
	// Use overlapping windows of up to three runes. This covers every byte
	// trigram without expanding all case spellings of the entire expression.
	width := min(3, len(literal))
	var windows []*gramCondition
	for i := 0; width != 0 && i+width <= len(literal); i++ {
		var alternatives []*gramCondition
		var spelling [3]rune
		foldedGrams(literal[i:i+width], spelling[:width], 0, &alternatives)
		windows = append(windows, combineGrams(false, alternatives))
	}
	return combineGrams(true, windows)
}

func foldedGrams(literal, spelling []rune, i int, alternatives *[]*gramCondition) {
	if i == len(literal) {
		*alternatives = append(*alternatives, byteGrams(string(spelling)))
		return
	}
	first := literal[i]
	for variant := first; ; {
		spelling[i] = variant
		foldedGrams(literal, spelling, i+1, alternatives)
		variant = unicode.SimpleFold(variant)
		if variant == first {
			return
		}
	}
}

func (q *gramCondition) terms(wanted map[uint32]struct{}) {
	if q == nil {
		return
	}
	if len(q.children) == 0 {
		wanted[q.gram] = struct{}{}
		return
	}
	for _, child := range q.children {
		child.terms(wanted)
	}
}

func (q *gramCondition) possible(postings map[uint32][]uint32, id uint32) bool {
	if len(q.children) == 0 {
		_, contains := slices.BinarySearch(postings[q.gram], id)
		return contains
	}
	for _, child := range q.children {
		if child.possible(postings, id) != q.all {
			return !q.all
		}
	}
	return q.all
}
