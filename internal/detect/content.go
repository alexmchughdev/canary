package detect

import (
	"github.com/alexmchughdev/foghorn/internal/cluster"
)

// ContentStatus is the verdict of classifying a live message against
// the channel's learned cluster fingerprints.
type ContentStatus string

const (
	StatusKnown           ContentStatus = "known"
	StatusUnknownPattern  ContentStatus = "unknown_pattern"
	StatusAbnormalContent ContentStatus = "abnormal_content"
)

// Verdict is the classification result for one message. ClusterID is -1
// when no cluster matched. MissingTokens is populated only for
// StatusAbnormalContent.
type Verdict struct {
	ClusterID     int
	Similarity    float64
	Status        ContentStatus
	MissingTokens []string
}

// Classifier matches live messages against a fixed set of cluster
// fingerprints learned during the boot-time scan. It shares the
// channel's Vectoriser so live IDF stays consistent with the corpus.
type Classifier struct {
	fingerprints []cluster.Fingerprint
	vectoriser   *cluster.Vectoriser
	threshold    float64 // min cosine similarity to consider a match
	stableRatio  float64 // min fraction of stable tokens that must be present
}

// ClassifierOptions configures the matching thresholds. Zero values
// fall back to the spec defaults.
type ClassifierOptions struct {
	Threshold   float64
	StableRatio float64
}

func NewClassifier(fps []cluster.Fingerprint, v *cluster.Vectoriser, opts ClassifierOptions) *Classifier {
	t := opts.Threshold
	if t == 0 {
		t = 0.5
	}
	r := opts.StableRatio
	if r == 0 {
		r = 0.8
	}
	return &Classifier{
		fingerprints: fps,
		vectoriser:   v,
		threshold:    t,
		stableRatio:  r,
	}
}

// Classify templatises and tokenises the message, vectorises it via
// the channel's IDF, and returns the nearest-fingerprint verdict.
//
// TODO(tuning): the threshold check below is the boundary between
// unknown_pattern (no cluster matched) and abnormal_content (cluster
// matched, missing stable tokens). On a low-density corpus a single
// structural-token swap (e.g. SUCCEEDED→FAILED) can pull similarity
// under threshold and produce unknown_pattern where the operator might
// reasonably expect abnormal_content. Both kinds raise alerts so the
// operational outcome is the same; the kind label is what changes.
// Tune Threshold against a real production corpus — synthetic data
// underweights token overlap and isn't the right substrate.
func (c *Classifier) Classify(text string) Verdict {
	tokens := cluster.Tokenize(cluster.Templatize(text))
	vec := c.vectoriser.Vectorise(tokens)

	bestID := -1
	bestSim := 0.0
	for _, fp := range c.fingerprints {
		sim := cluster.Cosine(vec, fp.Centroid)
		if sim > bestSim {
			bestSim = sim
			bestID = fp.ID
		}
	}

	if bestID == -1 || bestSim < c.threshold {
		return Verdict{ClusterID: -1, Similarity: bestSim, Status: StatusUnknownPattern}
	}

	matched := c.fingerprintByID(bestID)
	missing := missingStableTokens(tokens, matched.StableTokens)
	if len(matched.StableTokens) == 0 {
		return Verdict{ClusterID: bestID, Similarity: bestSim, Status: StatusKnown}
	}
	presentRatio := 1 - float64(len(missing))/float64(len(matched.StableTokens))
	if presentRatio < c.stableRatio {
		return Verdict{
			ClusterID:     bestID,
			Similarity:    bestSim,
			Status:        StatusAbnormalContent,
			MissingTokens: missing,
		}
	}
	return Verdict{ClusterID: bestID, Similarity: bestSim, Status: StatusKnown}
}

func (c *Classifier) fingerprintByID(id int) cluster.Fingerprint {
	for _, fp := range c.fingerprints {
		if fp.ID == id {
			return fp
		}
	}
	return cluster.Fingerprint{}
}

func missingStableTokens(tokens, stable []string) []string {
	if len(stable) == 0 {
		return nil
	}
	have := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		have[t] = struct{}{}
	}
	var out []string
	for _, s := range stable {
		if _, ok := have[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
