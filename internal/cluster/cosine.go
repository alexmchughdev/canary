package cluster

import "math"

// Cosine returns the cosine similarity between two sparse non-negative
// vectors. Result is in [0, 1]. Returns 0 if either vector has zero
// norm.
func Cosine(a, b map[string]float64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	// Iterate the smaller map to compute the dot product.
	small, large := a, b
	if len(b) < len(a) {
		small, large = b, a
	}
	var dot float64
	for k, va := range small {
		if vb, ok := large[k]; ok {
			dot += va * vb
		}
	}
	return dot / (norm(a) * norm(b))
}

func norm(v map[string]float64) float64 {
	var s float64
	for _, x := range v {
		s += x * x
	}
	if s == 0 {
		return 1 // avoid div-by-zero; callers guard the empty case
	}
	return math.Sqrt(s)
}
