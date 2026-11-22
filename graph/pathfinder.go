package graph

import (
	"container/heap"
	"errors"
	"sort"
)

var (
	ErrPathNotFound = errors.New("no path found between nodes")
	ErrInvalidNode  = errors.New("start or target node does not exist")
)

// EdgeWeightFunc assigns a numeric weight to a directed edge.
type EdgeWeightFunc func(from, to uint64, relation string) float64

// DefaultEdgeWeight provides a uniform edge cost of 1.0.
func DefaultEdgeWeight(from, to uint64, relation string) float64 {
	return 1.0
}

// PathResult contains the sequence of nodes, the relations connecting them, and total cost.
type PathResult struct {
	Nodes     []uint64
	Relations []string
	TotalCost float64
}

type dijkstraItem struct {
	node  uint64
	cost  float64
	depth int
	index int
}

type dijkstraPriorityQueue []*dijkstraItem

func (pq dijkstraPriorityQueue) Len() int           { return len(pq) }
func (pq dijkstraPriorityQueue) Less(i, j int) bool { return pq[i].cost < pq[j].cost }
func (pq dijkstraPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *dijkstraPriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*dijkstraItem)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *dijkstraPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// ShortestPath computes the shortest weighted path using Dijkstra's algorithm.
func (g *Graph) ShortestPath(from, to uint64, weightFn EdgeWeightFunc) (*PathResult, error) {
	return g.ShortestPathWithLimit(from, to, 0, 0, weightFn)
}

// ShortestPathWithLimit computes the shortest weighted path with optional maxDepth and maxBranching constraints.
// A maxDepth or maxBranching of 0 means unlimited.
func (g *Graph) ShortestPathWithLimit(from, to uint64, maxDepth, maxBranching int, weightFn EdgeWeightFunc) (*PathResult, error) {
	if weightFn == nil {
		weightFn = DefaultEdgeWeight
	}

	if from == to {
		return &PathResult{
			Nodes:     []uint64{from},
			Relations: []string{},
			TotalCost: 0,
		}, nil
	}

	dist := make(map[uint64]float64)
	parent := make(map[uint64]uint64)
	edgeRel := make(map[uint64]string)
	depth := make(map[uint64]int)

	pq := &dijkstraPriorityQueue{}
	heap.Init(pq)

	dist[from] = 0
	depth[from] = 0
	heap.Push(pq, &dijkstraItem{node: from, cost: 0, depth: 0})

	found := false

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*dijkstraItem)
		curr := item.node
		currCost := item.cost

		if currCost > dist[curr] {
			continue // Stale entry
		}

		if curr == to {
			found = true
			break
		}

		currDepth := depth[curr]
		if maxDepth > 0 && currDepth >= maxDepth {
			continue
		}

		neighbors := g.Outgoing(curr)
		if len(neighbors) == 0 {
			continue
		}

		type neighborWeight struct {
			node uint64
			rel  string
			w    float64
		}

		sorted := make([]neighborWeight, 0, len(neighbors))
		for _, n := range neighbors {
			rel, _ := g.EdgeRelation(curr, n)
			w := weightFn(curr, n, rel)
			sorted = append(sorted, neighborWeight{node: n, rel: rel, w: w})
		}

		if maxBranching > 0 && len(sorted) > maxBranching {
			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].w < sorted[j].w
			})
			sorted = sorted[:maxBranching]
		}

		for _, nw := range sorted {
			newCost := currCost + nw.w
			oldDist, exists := dist[nw.node]
			if !exists || newCost < oldDist {
				dist[nw.node] = newCost
				parent[nw.node] = curr
				edgeRel[nw.node] = nw.rel
				depth[nw.node] = currDepth + 1
				heap.Push(pq, &dijkstraItem{
					node:  nw.node,
					cost:  newCost,
					depth: currDepth + 1,
				})
			}
		}
	}

	if !found {
		return nil, ErrPathNotFound
	}

	// Reconstruct path
	var pathNodes []uint64
	var pathRels []string
	curr := to
	for curr != from {
		pathNodes = append([]uint64{curr}, pathNodes...)
		pathRels = append([]string{edgeRel[curr]}, pathRels...)
		curr = parent[curr]
	}
	pathNodes = append([]uint64{from}, pathNodes...)

	return &PathResult{
		Nodes:     pathNodes,
		Relations: pathRels,
		TotalCost: dist[to],
	}, nil
}

// FindReachable checks if 'to' is reachable from 'from' within maxDepth using BFS.
func (g *Graph) FindReachable(from, to uint64, maxDepth int) bool {
	if from == to {
		return true
	}
	if maxDepth <= 0 {
		maxDepth = 1000
	}

	visited := make(map[uint64]bool)
	queue := []uint64{from}
	visited[from] = true
	depth := make(map[uint64]int)
	depth[from] = 0

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == to {
			return true
		}

		if depth[current] >= maxDepth {
			continue
		}

		for _, next := range g.Outgoing(current) {
			if !visited[next] {
				visited[next] = true
				depth[next] = depth[current] + 1
				queue = append(queue, next)
			}
		}
	}

	return false
}

// LeastCommonAncestor finds the lowest common ancestor node between a and b within maxDepth.
func (g *Graph) LeastCommonAncestor(a, b uint64, maxDepth int) (uint64, bool) {
	if a == b {
		return a, true
	}
	if maxDepth <= 0 {
		maxDepth = 20
	}

	aAncestors := make(map[uint64]int)
	queue := []uint64{a}
	aAncestors[a] = 0
	d := 0

	for len(queue) > 0 && d < maxDepth {
		size := len(queue)
		for i := 0; i < size; i++ {
			curr := queue[0]
			queue = queue[1:]
			for _, caller := range g.Incoming(curr) {
				if _, exists := aAncestors[caller]; !exists {
					aAncestors[caller] = d + 1
					queue = append(queue, caller)
				}
			}
		}
		d++
	}

	bVisited := make(map[uint64]bool)
	queue = []uint64{b}
	bVisited[b] = true
	d = 0

	for len(queue) > 0 && d < maxDepth {
		size := len(queue)
		for i := 0; i < size; i++ {
			curr := queue[0]
			queue = queue[1:]
			if depthA, exists := aAncestors[curr]; exists && depthA == d {
				return curr, true
			}
			for _, caller := range g.Incoming(curr) {
				if !bVisited[caller] {
					bVisited[caller] = true
					queue = append(queue, caller)
				}
			}
		}
		d++
	}

	return 0, false
}

// GetCalleesRecursive retrieves all reachable outgoing nodes up to maxDepth.
func (g *Graph) GetCalleesRecursive(from uint64, maxDepth int) []uint64 {
	visited := make(map[uint64]bool)
	var result []uint64

	var dfs func(curr uint64, d int)
	dfs = func(curr uint64, d int) {
		if d >= maxDepth {
			return
		}
		for _, callee := range g.Outgoing(curr) {
			if !visited[callee] {
				visited[callee] = true
				result = append(result, callee)
				dfs(callee, d+1)
			}
		}
	}

	dfs(from, 0)
	return result
}

// GetCallersRecursive retrieves all reachable incoming nodes (predecessors) up to maxDepth.
func (g *Graph) GetCallersRecursive(to uint64, maxDepth int) []uint64 {
	visited := make(map[uint64]bool)
	var result []uint64

	var dfs func(curr uint64, d int)
	dfs = func(curr uint64, d int) {
		if d >= maxDepth {
			return
		}
		for _, caller := range g.Incoming(curr) {
			if !visited[caller] {
				visited[caller] = true
				result = append(result, caller)
				dfs(caller, d+1)
			}
		}
	}

	dfs(to, 0)
	return result
}
