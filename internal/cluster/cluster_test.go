package cluster

import "testing"

func TestBuild_endToEnd(t *testing.T) {
	corpus := []string{
		"deploy build #4521 succeeded in 12.3s",
		"deploy build #4522 succeeded in 11.8s",
		"deploy build #4523 succeeded in 13.1s",
		"health check ok cpu 23% mem 41%",
		"health check ok cpu 22% mem 39%",
		"health check ok cpu 24% mem 42%",
		"incident: api gateway timeout for user 9281",
		"totally unrelated standalone message",
	}
	clusters := Build(corpus, BuildOptions{Epsilon: 0.4, MinPts: 3})
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d: %+v", len(clusters), clusters)
	}

	sizes := map[int]int{}
	for _, c := range clusters {
		sizes[c.Size]++
	}
	if sizes[3] != 2 {
		t.Errorf("expected two clusters of size 3, got sizes %v", sizes)
	}

	// Each cluster should pick up its dominant content tokens as stable.
	hasToken := func(c Cluster, tok string) bool {
		for _, s := range c.StableTokens {
			if s == tok {
				return true
			}
		}
		return false
	}
	var deploy, health *Cluster
	for i := range clusters {
		c := &clusters[i]
		if hasToken(*c, "deploy") {
			deploy = c
		}
		if hasToken(*c, "health") {
			health = c
		}
	}
	if deploy == nil || health == nil {
		t.Fatalf("missing expected clusters: deploy=%v health=%v", deploy, health)
	}
	if !hasToken(*deploy, "succeeded") {
		t.Errorf("deploy cluster should mark 'succeeded' as stable: %+v", deploy.StableTokens)
	}
	if !hasToken(*health, "check") {
		t.Errorf("health cluster should mark 'check' as stable: %+v", health.StableTokens)
	}

	// Noise members must not appear in any cluster.
	for _, c := range clusters {
		for _, idx := range c.Members {
			if idx == 6 || idx == 7 {
				t.Errorf("noise message %d ended up in cluster %+v", idx, c)
			}
		}
	}
}

func TestBuild_empty(t *testing.T) {
	if got := Build(nil, BuildOptions{Epsilon: 0.4, MinPts: 3}); got != nil {
		t.Errorf("empty input should give nil result, got %v", got)
	}
}
