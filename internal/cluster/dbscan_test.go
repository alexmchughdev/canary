package cluster

import "testing"

func TestDBSCAN_twoDenseClusters(t *testing.T) {
	// Two clusters: {0,1,2} all share token "a", {3,4,5} all share "b".
	// {6} is a singleton with its own token, expected to be noise.
	vecs := []map[string]float64{
		{"a": 1.0, "x": 0.1},
		{"a": 1.0, "y": 0.1},
		{"a": 1.0, "z": 0.1},
		{"b": 1.0, "x": 0.1},
		{"b": 1.0, "y": 0.1},
		{"b": 1.0, "z": 0.1},
		{"c": 1.0},
	}
	labels := DBSCAN(vecs, Params{Epsilon: 0.4, MinPts: 3})

	// Members 0,1,2 should share an id; 3,4,5 should share another id.
	if labels[0] == -1 || labels[1] == -1 || labels[2] == -1 {
		t.Errorf("expected first cluster to be assigned, got %v", labels)
	}
	if labels[0] != labels[1] || labels[1] != labels[2] {
		t.Errorf("members 0-2 should share a cluster, got %v", labels)
	}
	if labels[3] != labels[4] || labels[4] != labels[5] {
		t.Errorf("members 3-5 should share a cluster, got %v", labels)
	}
	if labels[0] == labels[3] {
		t.Errorf("the two groups must not share an id, got %v", labels)
	}
	if labels[6] != -1 {
		t.Errorf("singleton should be noise, got %v", labels[6])
	}
}

func TestDBSCAN_allNoiseWhenMinPtsTooHigh(t *testing.T) {
	vecs := []map[string]float64{
		{"a": 1}, {"a": 1}, {"a": 1},
	}
	labels := DBSCAN(vecs, Params{Epsilon: 0.1, MinPts: 5})
	for i, l := range labels {
		if l != -1 {
			t.Errorf("vec %d: expected noise, got %d", i, l)
		}
	}
}

func TestDBSCAN_empty(t *testing.T) {
	labels := DBSCAN(nil, Params{Epsilon: 0.4, MinPts: 3})
	if len(labels) != 0 {
		t.Errorf("expected empty result, got %v", labels)
	}
}
