package graph

// DetectCycles finds all elementary cycles in the graph using DFS with a recursion stack.
// Cycles are returned as slices of uint64 node IDs representing the cycle loop: [A, B, C, A].
func (g *Graph) DetectCycles() [][]uint64 {
	var cycles [][]uint64
	visited := make(map[uint64]bool)
	inStack := make(map[uint64]bool)
	path := []uint64{}

	var dfs func(node uint64) bool
	dfs = func(node uint64) bool {
		if inStack[node] {
			cycleStart := -1
			for i, n := range path {
				if n == node {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				cycle := make([]uint64, len(path)-cycleStart)
				copy(cycle, path[cycleStart:])
				cycle = append(cycle, node)
				cycles = append(cycles, cycle)
			}
			return true
		}
		if visited[node] {
			return false
		}

		visited[node] = true
		inStack[node] = true
		path = append(path, node)

		for _, callee := range g.Outgoing(node) {
			dfs(callee)
		}

		path = path[:len(path)-1]
		inStack[node] = false
		return false
	}

	for _, node := range g.Nodes() {
		if !visited[node] {
			dfs(node)
		}
	}

	return cycles
}

// DetectCyclesLabels returns all cycles resolved to their string labels.
func (g *Graph) DetectCyclesLabels() [][]string {
	idCycles := g.DetectCycles()
	stringCycles := make([][]string, 0, len(idCycles))

	for _, c := range idCycles {
		labels := make([]string, len(c))
		for i, id := range c {
			lbl, err := g.ResolveID(id)
			if err != nil {
				lbl = ""
			}
			labels[i] = lbl
		}
		stringCycles = append(stringCycles, labels)
	}

	return stringCycles
}
