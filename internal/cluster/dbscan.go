package cluster

// Params controls DBSCAN's neighbourhood radius and core-point
// threshold. Distance is 1 - Cosine, in [0, 1].
type Params struct {
	Epsilon float64
	MinPts  int
}

// DBSCAN runs density-based clustering and returns one cluster id per
// input vector, in input order. Noise points get id -1; assigned ids
// start at 0.
func DBSCAN(vectors []map[string]float64, p Params) []int {
	n := len(vectors)
	labels := make([]int, n)
	for i := range labels {
		labels[i] = unlabelled
	}
	cluster := 0
	for i := 0; i < n; i++ {
		if labels[i] != unlabelled {
			continue
		}
		neighbours := regionQuery(vectors, i, p.Epsilon)
		if len(neighbours) < p.MinPts {
			labels[i] = noise
			continue
		}
		expand(vectors, labels, i, neighbours, cluster, p)
		cluster++
	}
	return labels
}

const (
	unlabelled = -2
	noise      = -1
)

func expand(vectors []map[string]float64, labels []int, seed int, seedNeighbours []int, cluster int, p Params) {
	labels[seed] = cluster
	queue := append([]int(nil), seedNeighbours...)
	for i := 0; i < len(queue); i++ {
		q := queue[i]
		if labels[q] == noise {
			labels[q] = cluster
		}
		if labels[q] != unlabelled {
			continue
		}
		labels[q] = cluster
		neighbours := regionQuery(vectors, q, p.Epsilon)
		if len(neighbours) >= p.MinPts {
			queue = append(queue, neighbours...)
		}
	}
}

// regionQuery returns indices of vectors within epsilon of vectors[i],
// including i itself. Naive O(n) per call; sufficient for the corpus
// sizes this engine targets.
func regionQuery(vectors []map[string]float64, i int, epsilon float64) []int {
	var out []int
	for j := range vectors {
		if 1-Cosine(vectors[i], vectors[j]) <= epsilon {
			out = append(out, j)
		}
	}
	return out
}
