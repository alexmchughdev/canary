package cluster

import "testing"

func TestTemplatize(t *testing.T) {
	cases := map[string]string{
		"build #4521 succeeded":                "build #<num> succeeded",
		"deploy took 12.3s":                    "deploy took <dur>",
		"see https://example.com/x for logs":   "see <url> for logs",
		"commit a1b2c3d4 merged":               "commit <sha> merged",
		"timestamp 2024-01-02T03:04:05Z fired": "timestamp <ts> fired",
		"host 10.0.0.42 down":                  "host <ip> down",
		"value 3.14 vs 100":                    "value <num> vs <num>",
	}
	for in, want := range cases {
		if got := Templatize(in); got != want {
			t.Errorf("Templatize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTemplatize_orderURLBeforeSHA(t *testing.T) {
	in := "see https://github.com/owner/repo/commit/a1b2c3d4e5f6 now"
	if got := Templatize(in); got != "see <url> now" {
		t.Errorf("URL must consume hex tail before SHA matches; got %q", got)
	}
}
