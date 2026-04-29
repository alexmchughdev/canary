// Package cluster provides TF-IDF vectorisation, cosine similarity,
// DBSCAN clustering, and fingerprinting for short messages.
package cluster

import "regexp"

var (
	reURL      = regexp.MustCompile(`https?://\S+`)
	reISOTime  = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?`)
	reDuration = regexp.MustCompile(`\d+(\.\d+)?(ms|s|m|h)\b`)
	reSHA      = regexp.MustCompile(`\b[a-f0-9]{7,40}\b`)
	reUUID     = regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reIPv4     = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	reNumber   = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
)

// Templatize replaces variable substrings with stable placeholders so
// otherwise identical messages collapse to the same shape. Order
// matters: URLs are replaced before SHAs because URLs contain hex.
func Templatize(s string) string {
	s = reURL.ReplaceAllString(s, "<url>")
	s = reISOTime.ReplaceAllString(s, "<ts>")
	s = reDuration.ReplaceAllString(s, "<dur>")
	s = reSHA.ReplaceAllString(s, "<sha>")
	s = reUUID.ReplaceAllString(s, "<uuid>")
	s = reIPv4.ReplaceAllString(s, "<ip>")
	s = reNumber.ReplaceAllString(s, "<num>")
	return s
}
