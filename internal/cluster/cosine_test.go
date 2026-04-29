package cluster

import (
	"math"
	"testing"
)

func TestCosine_identical(t *testing.T) {
	a := map[string]float64{"x": 1, "y": 2}
	if got := Cosine(a, a); math.Abs(got-1) > 1e-9 {
		t.Errorf("identical vectors should give 1, got %v", got)
	}
}

func TestCosine_orthogonal(t *testing.T) {
	a := map[string]float64{"x": 1}
	b := map[string]float64{"y": 1}
	if got := Cosine(a, b); got != 0 {
		t.Errorf("disjoint keys should give 0, got %v", got)
	}
}

func TestCosine_partialOverlap(t *testing.T) {
	a := map[string]float64{"x": 1, "y": 1}
	b := map[string]float64{"x": 1, "z": 1}
	got := Cosine(a, b)
	if got <= 0 || got >= 1 {
		t.Errorf("partial overlap should be in (0, 1), got %v", got)
	}
}

func TestCosine_emptyVector(t *testing.T) {
	if got := Cosine(nil, map[string]float64{"x": 1}); got != 0 {
		t.Errorf("empty input should give 0, got %v", got)
	}
}
