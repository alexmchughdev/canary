package cluster

import "math"

// Vectoriser holds the IDF table learned from a corpus and produces
// sparse TF-IDF vectors for new documents. Fit must be called before
// Vectorise.
type Vectoriser struct {
	idf map[string]float64
	df  map[string]int
	n   int
}

func NewVectoriser() *Vectoriser {
	return &Vectoriser{
		idf: map[string]float64{},
		df:  map[string]int{},
	}
}

// Fit accumulates document frequencies over docs and computes a
// smoothed log IDF: log((N+1)/(df+1)) + 1.
func (v *Vectoriser) Fit(docs [][]string) {
	v.n = len(docs)
	v.df = map[string]int{}
	for _, doc := range docs {
		seen := map[string]struct{}{}
		for _, tok := range doc {
			if _, ok := seen[tok]; ok {
				continue
			}
			seen[tok] = struct{}{}
			v.df[tok]++
		}
	}
	v.idf = make(map[string]float64, len(v.df))
	for tok, df := range v.df {
		v.idf[tok] = math.Log(float64(v.n+1)/float64(df+1)) + 1
	}
}

// Vectorise returns a sparse TF-IDF vector for one document. TF is the
// raw token count divided by the document length. Tokens unseen during
// Fit are skipped.
func (v *Vectoriser) Vectorise(doc []string) map[string]float64 {
	out := map[string]float64{}
	if len(doc) == 0 {
		return out
	}
	counts := map[string]int{}
	for _, tok := range doc {
		counts[tok]++
	}
	docLen := float64(len(doc))
	for tok, c := range counts {
		idf, ok := v.idf[tok]
		if !ok {
			continue
		}
		tf := float64(c) / docLen
		out[tok] = tf * idf
	}
	return out
}
