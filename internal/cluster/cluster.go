package cluster

// Cluster is the public result of Build for one assigned cluster id.
// Members holds indices into the original messages slice.
type Cluster struct {
	Fingerprint
	Members []int
}

// BuildOptions controls the DBSCAN parameters used by Build.
type BuildOptions struct {
	Epsilon float64
	MinPts  int
}

// Build templatises, tokenises, vectorises, clusters, and fingerprints
// the input messages. Noise points are excluded from the result.
func Build(messages []string, opts BuildOptions) []Cluster {
	if len(messages) == 0 {
		return nil
	}

	tokens := make([][]string, len(messages))
	for i, m := range messages {
		tokens[i] = Tokenize(Templatize(m))
	}

	v := NewVectoriser()
	v.Fit(tokens)

	vecs := make([]map[string]float64, len(tokens))
	for i, doc := range tokens {
		vecs[i] = v.Vectorise(doc)
	}

	labels := DBSCAN(vecs, Params{Epsilon: opts.Epsilon, MinPts: opts.MinPts})

	groups := map[int][]int{}
	for i, l := range labels {
		if l < 0 {
			continue
		}
		groups[l] = append(groups[l], i)
	}

	out := make([]Cluster, 0, len(groups))
	for id, members := range groups {
		memberVecs := make([]map[string]float64, len(members))
		memberTokens := make([][]string, len(members))
		for i, idx := range members {
			memberVecs[i] = vecs[idx]
			memberTokens[i] = tokens[idx]
		}
		out = append(out, Cluster{
			Fingerprint: buildFingerprint(id, memberVecs, memberTokens, messages[members[0]]),
			Members:     members,
		})
	}
	return out
}
