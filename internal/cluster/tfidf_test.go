package cluster

import (
	"math"
	"testing"
)

func TestVectoriser_Fit_idfRaresHigher(t *testing.T) {
	docs := [][]string{
		{"build", "succeeded"},
		{"build", "failed"},
		{"build", "succeeded"},
		{"build", "succeeded"},
	}
	v := NewVectoriser()
	v.Fit(docs)

	if v.idf["build"] >= v.idf["failed"] {
		t.Errorf("idf(build)=%v should be lower than idf(failed)=%v",
			v.idf["build"], v.idf["failed"])
	}
}

func TestVectoriser_Vectorise(t *testing.T) {
	docs := [][]string{
		{"build", "succeeded"},
		{"build", "failed"},
	}
	v := NewVectoriser()
	v.Fit(docs)

	vec := v.Vectorise([]string{"build", "succeeded"})
	if _, ok := vec["build"]; !ok {
		t.Errorf("expected build in vector: %v", vec)
	}
	if _, ok := vec["succeeded"]; !ok {
		t.Errorf("expected succeeded in vector: %v", vec)
	}
	for _, w := range vec {
		if math.IsNaN(w) || math.IsInf(w, 0) {
			t.Errorf("non-finite weight: %v", vec)
		}
	}
}

func TestVectoriser_Vectorise_unknownTokenSkipped(t *testing.T) {
	v := NewVectoriser()
	v.Fit([][]string{{"build"}})
	vec := v.Vectorise([]string{"unknown", "build"})
	if _, ok := vec["unknown"]; ok {
		t.Errorf("unknown token should not appear: %v", vec)
	}
}

func TestVectoriser_emptyDoc(t *testing.T) {
	v := NewVectoriser()
	v.Fit([][]string{{"a", "b"}})
	if got := v.Vectorise(nil); len(got) != 0 {
		t.Errorf("empty input should give empty vec, got %v", got)
	}
}
