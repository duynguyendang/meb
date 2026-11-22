package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/duynguyendang/meb"
)

// Graph represents a directed graph operating on compact uint64 dictionary IDs.
type Graph struct {
	mu          sync.RWMutex
	outgoing    map[uint64][]uint64
	incoming    map[uint64][]uint64
	edges       map[uint64]map[uint64]string // from -> to -> relation
	nodeSet     map[uint64]struct{}
	store       *meb.MEBStore
	labelToID   map[string]uint64
	idToLabel   map[uint64]string
	nextLocalID uint64
}

// New creates an empty Graph.
func New() *Graph {
	return &Graph{
		outgoing:    make(map[uint64][]uint64),
		incoming:    make(map[uint64][]uint64),
		edges:       make(map[uint64]map[uint64]string),
		nodeSet:     make(map[uint64]struct{}),
		labelToID:   make(map[string]uint64),
		idToLabel:   make(map[uint64]string),
		nextLocalID: 1000000000, // fallback ID generator for synthetic nodes
	}
}

// NewFromStore constructs a Graph by scanning specified predicates from a MEBStore.
// If no predicates are specified, all facts in the store will be scanned.
func NewFromStore(ctx context.Context, store *meb.MEBStore, predicates ...string) (*Graph, error) {
	g := New()
	g.store = store

	scanPred := func(pred string) {
		for fact, err := range store.ScanContext(ctx, "", pred, "") {
			if err != nil {
				continue
			}
			objStr, ok := fact.Object.(string)
			if !ok {
				continue
			}

			sID := g.getOrAssignID(fact.Subject)
			oID := g.getOrAssignID(objStr)

			g.AddEdge(sID, oID, fact.Predicate)
		}
	}

	if len(predicates) == 0 {
		scanPred("")
	} else {
		for _, pred := range predicates {
			scanPred(pred)
		}
	}

	return g, nil
}

func (g *Graph) getOrAssignID(label string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if id, ok := g.labelToID[label]; ok {
		return id
	}

	if g.store != nil {
		if id, found := g.store.LookupID(label); found {
			g.labelToID[label] = id
			g.idToLabel[id] = label
			return id
		}
	}

	g.nextLocalID++
	id := g.nextLocalID
	g.labelToID[label] = id
	g.idToLabel[id] = label
	return id
}

// AddEdge adds a directed edge from 'from' to 'to' with an optional relation predicate.
func (g *Graph) AddEdge(from, to uint64, relation string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nodeSet[from] = struct{}{}
	g.nodeSet[to] = struct{}{}

	if g.edges[from] == nil {
		g.edges[from] = make(map[uint64]string)
	}
	g.edges[from][to] = relation

	// Maintain outgoing adjacency
	outList := g.outgoing[from]
	hasOut := false
	for _, n := range outList {
		if n == to {
			hasOut = true
			break
		}
	}
	if !hasOut {
		g.outgoing[from] = append(outList, to)
	}

	// Maintain incoming adjacency
	inList := g.incoming[to]
	hasIn := false
	for _, n := range inList {
		if n == from {
			hasIn = true
			break
		}
	}
	if !hasIn {
		g.incoming[to] = append(inList, from)
	}
}

// AddEdgeByLabel adds a directed edge by string labels, mapping them to uint64 IDs.
func (g *Graph) AddEdgeByLabel(fromLabel, toLabel, relation string) (uint64, uint64) {
	fromID := g.getOrAssignID(fromLabel)
	toID := g.getOrAssignID(toLabel)
	g.AddEdge(fromID, toID, relation)
	return fromID, toID
}

// Nodes returns all unique node IDs in the graph.
func (g *Graph) Nodes() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]uint64, 0, len(g.nodeSet))
	for n := range g.nodeSet {
		nodes = append(nodes, n)
	}
	return nodes
}

// NodeCount returns the total number of unique nodes.
func (g *Graph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodeSet)
}

// EdgeCount returns the total number of directed edges.
func (g *Graph) EdgeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, targets := range g.edges {
		count += len(targets)
	}
	return count
}

// Outgoing returns the adjacent successor node IDs.
func (g *Graph) Outgoing(node uint64) []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.outgoing[node]
}

// Incoming returns the predecessor node IDs.
func (g *Graph) Incoming(node uint64) []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.incoming[node]
}

// EdgeRelation returns the relation label for a directed edge from 'from' to 'to'.
func (g *Graph) EdgeRelation(from, to uint64) (string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	targets, ok := g.edges[from]
	if !ok {
		return "", false
	}
	rel, ok := targets[to]
	return rel, ok
}

// LookupID resolves a string label to a uint64 ID.
func (g *Graph) LookupID(label string) (uint64, bool) {
	g.mu.RLock()
	id, ok := g.labelToID[label]
	g.mu.RUnlock()
	if ok {
		return id, true
	}

	if g.store != nil {
		if sid, found := g.store.LookupID(label); found {
			g.mu.Lock()
			g.labelToID[label] = sid
			g.idToLabel[sid] = label
			g.mu.Unlock()
			return sid, true
		}
	}
	return 0, false
}

// ResolveID resolves a uint64 ID back to its string label.
func (g *Graph) ResolveID(id uint64) (string, error) {
	g.mu.RLock()
	label, ok := g.idToLabel[id]
	g.mu.RUnlock()
	if ok {
		return label, nil
	}

	if g.store != nil {
		lbl, err := g.store.ResolveID(id)
		if err == nil {
			g.mu.Lock()
			g.idToLabel[id] = lbl
			g.labelToID[lbl] = id
			g.mu.Unlock()
			return lbl, nil
		}
	}
	return "", fmt.Errorf("id %d not found", id)
}
