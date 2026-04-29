package cluster

import (
	"sort"
	"testing"
)

func TestCentroid_meanOfMembers(t *testing.T) {
	vecs := []map[string]float64{
		{"a": 2, "b": 4},
		{"a": 4, "b": 8},
	}
	c := centroid(vecs)
	if c["a"] != 3 {
		t.Errorf("centroid[a] = %v, want 3", c["a"])
	}
	if c["b"] != 6 {
		t.Errorf("centroid[b] = %v, want 6", c["b"])
	}
}

func TestStableTokens_threshold(t *testing.T) {
	docs := [][]string{
		{"build", "succeeded", "fast"},
		{"build", "succeeded", "slow"},
		{"build", "succeeded", "again"},
		{"build", "failed", "once"},
	}
	got := stableTokens(docs, 0.8)
	sort.Strings(got)
	want := []string{"build", "succeeded"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("stableTokens = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("stableTokens[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStableTokens_emptyDocs(t *testing.T) {
	if got := stableTokens(nil, 0.8); got != nil {
		t.Errorf("empty input should give nil, got %v", got)
	}
}
