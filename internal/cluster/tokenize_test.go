package cluster

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	cases := map[string][]string{
		"build <num> succeeded in <dur>": {"build", "<num>", "succeeded", "<dur>"},
		"the api gateway is down":        {"api", "gateway", "down"},
		"a single i":                     {"single"},
		"Mixed CASE Tokens":              {"mixed", "case", "tokens"},
		"":                               {},
	}
	for in, want := range cases {
		got := Tokenize(in)
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Tokenize(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestTokenize_keepsTemplateTokens(t *testing.T) {
	got := Tokenize("commit <sha> at <ts>")
	want := []string{"commit", "<sha>", "<ts>"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}
