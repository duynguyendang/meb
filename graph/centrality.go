package graph

// DegreeCentrality calculates the in-degree and out-degree for every node in the graph.
func (g *Graph) DegreeCentrality() (inDegree map[uint64]int, outDegree map[uint64]int) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	inDegree = make(map[uint64]int, len(g.nodeSet))
	outDegree = make(map[uint64]int, len(g.nodeSet))

	for node := range g.nodeSet {
		inDegree[node] = len(g.incoming[node])
		outDegree[node] = len(g.outgoing[node])
	}
	return inDegree, outDegree
}

type edgeInIndex struct {
	srcIdx int
	weight float64
}

// PageRank computes the PageRank scores for all nodes in the graph using power iteration.
// The resulting scores are normalized such that the maximum score is 1.0 (or empty if N=0).
func (g *Graph) PageRank(damping float64, iterations int) map[uint64]float64 {
	if iterations <= 0 {
		iterations = 20
	}
	if damping <= 0 {
		damping = 0.85
	}

	nodes := g.Nodes()
	N := len(nodes)
	if N == 0 {
		return make(map[uint64]float64)
	}

	nodeIndex := make(map[uint64]int, N)
	for i, node := range nodes {
		nodeIndex[node] = i
	}

	ranks := make([]float64, N)
	initialRank := 1.0 / float64(N)
	for i := range ranks {
		ranks[i] = initialRank
	}

	outDegree := make([]int, N)
	for i, node := range nodes {
		outDegree[i] = len(g.Outgoing(node))
	}

	inEdges := make([][]edgeInIndex, N)
	for j, node := range nodes {
		if outDegree[j] == 0 {
			continue
		}
		weight := 1.0 / float64(outDegree[j])
		for _, target := range g.Outgoing(node) {
			if targetIdx, ok := nodeIndex[target]; ok {
				inEdges[targetIdx] = append(inEdges[targetIdx], edgeInIndex{srcIdx: j, weight: weight})
			}
		}
	}

	baseVal := (1.0 - damping) / float64(N)
	for iter := 0; iter < iterations; iter++ {
		newRanks := make([]float64, N)
		for i := 0; i < N; i++ {
			var sum float64
			for _, e := range inEdges[i] {
				sum += damping * ranks[e.srcIdx] * e.weight
			}
			newRanks[i] = baseVal + sum
		}
		ranks = newRanks
	}

	centrality := make(map[uint64]float64, N)
	maxRank := 0.0
	for i, node := range nodes {
		centrality[node] = ranks[i]
		if ranks[i] > maxRank {
			maxRank = ranks[i]
		}
	}

	if maxRank > 0 {
		for node := range centrality {
			centrality[node] /= maxRank
		}
	}

	return centrality
}
