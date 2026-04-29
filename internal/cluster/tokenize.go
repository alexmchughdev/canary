package cluster

import (
	"regexp"
	"strings"
)

// reSplit splits on any run of characters that aren't alnum, underscore,
// or angle brackets. Angle brackets are kept so template tokens like
// <num> survive intact.
var reSplit = regexp.MustCompile(`[^a-z0-9_<>]+`)

var stopwords = map[string]struct{}{
	"the": {}, "a": {}, "an": {}, "and": {}, "or": {}, "of": {}, "to": {},
	"in": {}, "on": {}, "at": {}, "is": {}, "are": {}, "was": {}, "were": {},
	"be": {}, "with": {}, "for": {}, "from": {}, "by": {}, "that": {},
	"this": {}, "it": {}, "its": {},
}

// Tokenize lowercases, splits on non-alnum (keeping <template> tokens
// intact), and drops single-char tokens and stopwords. Caller is
// expected to have already run Templatize.
func Tokenize(s string) []string {
	lower := strings.ToLower(s)
	parts := reSplit.Split(lower, -1)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if len(p) <= 1 {
			continue
		}
		if _, stop := stopwords[p]; stop {
			continue
		}
		out = append(out, p)
	}
	return out
}
