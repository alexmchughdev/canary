package detect

import (
	"testing"

	"github.com/alexmchughdev/foghorn/internal/cluster"
)

// trainCorpus builds a tiny corpus + fingerprints + vectoriser the
// classifier can match against. Two stable patterns: deploy-succeeded
// and health-check-ok.
func trainCorpus(t *testing.T) ([]cluster.Fingerprint, *cluster.Vectoriser) {
	t.Helper()
	corpus := []string{
		"deploy build #4521 succeeded in 12.3s",
		"deploy build #4522 succeeded in 11.8s",
		"deploy build #4523 succeeded in 13.1s",
		"deploy build #4524 succeeded in 12.7s",
		"health check ok cpu 23% mem 41%",
		"health check ok cpu 22% mem 39%",
		"health check ok cpu 24% mem 42%",
		"health check ok cpu 21% mem 40%",
	}
	tokens := make([][]string, len(corpus))
	for i, m := range corpus {
		tokens[i] = cluster.Tokenize(cluster.Templatize(m))
	}
	v := cluster.NewVectoriser()
	v.Fit(tokens)

	clusters := cluster.Build(corpus, cluster.BuildOptions{Epsilon: 0.4, MinPts: 3})
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(clusters))
	}
	fps := make([]cluster.Fingerprint, len(clusters))
	for i, c := range clusters {
		fps[i] = c.Fingerprint
	}
	return fps, v
}

func TestClassify_known(t *testing.T) {
	fps, v := trainCorpus(t)
	c := NewClassifier(fps, v, ClassifierOptions{})
	got := c.Classify("deploy build #9999 succeeded in 8.2s")
	if got.Status != StatusKnown {
		t.Errorf("expected known, got %+v", got)
	}
	if got.ClusterID < 0 {
		t.Errorf("expected matched cluster, got %d", got.ClusterID)
	}
}

func TestClassify_unknownPattern(t *testing.T) {
	fps, v := trainCorpus(t)
	c := NewClassifier(fps, v, ClassifierOptions{})
	got := c.Classify("incident api gateway timeout for user 9281")
	if got.Status != StatusUnknownPattern {
		t.Errorf("expected unknown_pattern, got %+v", got)
	}
}

func TestClassify_abnormalContent(t *testing.T) {
	fps, v := trainCorpus(t)
	c := NewClassifier(fps, v, ClassifierOptions{StableRatio: 0.9})
	// Same shape as deploy cluster but missing the "succeeded" stable
	// token; should still match the cluster on cosine but trip the
	// stable-token check.
	got := c.Classify("deploy build #9999 something something else")
	if got.ClusterID < 0 {
		t.Fatalf("expected to match a cluster, got %+v", got)
	}
	if got.Status == StatusKnown {
		t.Errorf("expected non-known verdict, got %+v", got)
	}
}

func TestMissingStableTokens(t *testing.T) {
	got := missingStableTokens(
		[]string{"deploy", "build", "<num>"},
		[]string{"deploy", "succeeded"},
	)
	if len(got) != 1 || got[0] != "succeeded" {
		t.Errorf("got %v, want [succeeded]", got)
	}
}
