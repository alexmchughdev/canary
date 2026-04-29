package cluster

// Fingerprint summarises one cluster: the centroid vector for similarity
// matching, the stable token set that defines the cluster's structural
// shape, and a sample message for human inspection.
type Fingerprint struct {
	ID            int
	Size          int
	Centroid      map[string]float64
	StableTokens  []string
	SampleMessage string
}

// stableRatio is the prevalence threshold for a token to count as
// structurally stable within a cluster.
const stableRatio = 0.8

// buildFingerprint computes a centroid and stable-token set for the
// given member vectors and tokenised documents. members and tokens
// must align by index.
func buildFingerprint(id int, memberVecs []map[string]float64, memberTokens [][]string, sample string) Fingerprint {
	return Fingerprint{
		ID:            id,
		Size:          len(memberVecs),
		Centroid:      centroid(memberVecs),
		StableTokens:  stableTokens(memberTokens, stableRatio),
		SampleMessage: sample,
	}
}

// centroid is the per-key mean of the input vectors.
func centroid(vecs []map[string]float64) map[string]float64 {
	if len(vecs) == 0 {
		return map[string]float64{}
	}
	sum := map[string]float64{}
	for _, v := range vecs {
		for k, x := range v {
			sum[k] += x
		}
	}
	n := float64(len(vecs))
	for k := range sum {
		sum[k] /= n
	}
	return sum
}

// stableTokens returns tokens present in at least ratio of the input
// documents. Each document contributes once per unique token regardless
// of repetition.
func stableTokens(docs [][]string, ratio float64) []string {
	if len(docs) == 0 {
		return nil
	}
	counts := map[string]int{}
	for _, doc := range docs {
		seen := map[string]struct{}{}
		for _, tok := range doc {
			if _, ok := seen[tok]; ok {
				continue
			}
			seen[tok] = struct{}{}
			counts[tok]++
		}
	}
	threshold := int(ratio * float64(len(docs)))
	if threshold < 1 {
		threshold = 1
	}
	var out []string
	for tok, c := range counts {
		if c >= threshold {
			out = append(out, tok)
		}
	}
	return out
}
