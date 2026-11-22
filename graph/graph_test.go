package graph

import (
	"context"
	"testing"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
)

func TestGraph_BasicEdges(t *testing.T) {
	g := New()
	idA, idB := g.AddEdgeByLabel("A", "B", "calls")
	_, idC := g.AddEdgeByLabel("B", "C", "calls")

	if g.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", g.NodeCount())
	}
	if g.EdgeCount() != 2 {
		t.Fatalf("expected 2 edges, got %d", g.EdgeCount())
	}

	outA := g.Outgoing(idA)
	if len(outA) != 1 || outA[0] != idB {
		t.Fatalf("expected outgoing of A to be B, got %v", outA)
	}

	inC := g.Incoming(idC)
	if len(inC) != 1 || inC[0] != idB {
		t.Fatalf("expected incoming of C to be B, got %v", inC)
	}

	lblA, err := g.ResolveID(idA)
	if err != nil || lblA != "A" {
		t.Fatalf("failed to resolve A label: %v", err)
	}
}

func TestGraph_ShortestPathDijkstra(t *testing.T) {
	g := New()
	idA, idB := g.AddEdgeByLabel("A", "B", "calls")
	g.AddEdgeByLabel("A", "C", "imports")
	_, _ = g.LookupID("C")
	g.AddEdgeByLabel("B", "D", "calls")
	idD, _ := g.LookupID("D")
	g.AddEdgeByLabel("C", "D", "calls")

	// Custom weight: calls=1.0, imports=5.0
	weightFn := func(from, to uint64, rel string) float64 {
		if rel == "calls" {
			return 1.0
		}
		return 5.0
	}

	res, err := g.ShortestPath(idA, idD, weightFn)
	if err != nil {
		t.Fatalf("ShortestPath failed: %v", err)
	}

	// Shortest weighted path should be A -> B -> D (cost 1+1=2), not A -> C -> D (cost 5+1=6)
	if res.TotalCost != 2.0 {
		t.Fatalf("expected cost 2.0, got %f", res.TotalCost)
	}
	if len(res.Nodes) != 3 || res.Nodes[0] != idA || res.Nodes[1] != idB || res.Nodes[2] != idD {
		t.Fatalf("unexpected path nodes: %v", res.Nodes)
	}
	if len(res.Relations) != 2 || res.Relations[0] != "calls" || res.Relations[1] != "calls" {
		t.Fatalf("unexpected path relations: %v", res.Relations)
	}
}

func TestGraph_FindReachable(t *testing.T) {
	g := New()
	idA, _ := g.AddEdgeByLabel("A", "B", "calls")
	g.AddEdgeByLabel("B", "C", "calls")
	idC, _ := g.LookupID("C")
	idD := g.getOrAssignID("D") // isolated

	if !g.FindReachable(idA, idC, 5) {
		t.Fatalf("expected C to be reachable from A")
	}
	if g.FindReachable(idA, idD, 5) {
		t.Fatalf("expected D NOT to be reachable from A")
	}
	if g.FindReachable(idA, idC, 1) {
		t.Fatalf("expected C NOT to be reachable from A with maxDepth=1")
	}
}

func TestGraph_LeastCommonAncestor(t *testing.T) {
	g := New()
	// Ancestor -> Child1, Ancestor -> Child2
	anc, c1 := g.AddEdgeByLabel("Root", "Child1", "calls")
	_, c2 := g.AddEdgeByLabel("Root", "Child2", "calls")

	lca, found := g.LeastCommonAncestor(c1, c2, 5)
	if !found {
		t.Fatalf("expected LCA to be found")
	}
	if lca != anc {
		t.Fatalf("expected LCA to be Root (%d), got %d", anc, lca)
	}
}

func TestGraph_DetectCycles(t *testing.T) {
	g := New()
	// Cycle: A -> B -> C -> A
	g.AddEdgeByLabel("A", "B", "calls")
	g.AddEdgeByLabel("B", "C", "calls")
	g.AddEdgeByLabel("C", "A", "calls")
	// Non-cycle branch: C -> D
	g.AddEdgeByLabel("C", "D", "calls")

	cycles := g.DetectCycles()
	if len(cycles) != 1 {
		t.Fatalf("expected 1 cycle, got %d: %v", len(cycles), cycles)
	}

	labels := g.DetectCyclesLabels()
	if len(labels) != 1 {
		t.Fatalf("expected 1 label cycle, got %d", len(labels))
	}
	c := labels[0]
	if len(c) != 4 || c[0] != c[3] {
		t.Fatalf("expected closed cycle loop [A, B, C, A], got: %v", c)
	}
}

func TestGraph_CentralityAndPageRank(t *testing.T) {
	g := New()
	// Hub node B receives links from A, C, D
	g.AddEdgeByLabel("A", "B", "calls")
	g.AddEdgeByLabel("C", "B", "calls")
	g.AddEdgeByLabel("D", "B", "calls")
	// B calls E
	g.AddEdgeByLabel("B", "E", "calls")

	idB, _ := g.LookupID("B")
	inDeg, outDeg := g.DegreeCentrality()
	if inDeg[idB] != 3 {
		t.Fatalf("expected B in-degree 3, got %d", inDeg[idB])
	}
	if outDeg[idB] != 1 {
		t.Fatalf("expected B out-degree 1, got %d", outDeg[idB])
	}

	ranks := g.PageRank(0.85, 30)
	if len(ranks) != 5 {
		t.Fatalf("expected 5 node ranks, got %d", len(ranks))
	}

	idE, _ := g.LookupID("E")
	// E receives from hub B, so E should have high rank
	if ranks[idE] <= 0 {
		t.Fatalf("expected positive rank for E, got %f", ranks[idE])
	}
}

func TestGraph_NewFromStore(t *testing.T) {
	cfg := &store.Config{InMemory: true}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	_ = s.AddFact(meb.Fact{Subject: "main.go", Predicate: "calls", Object: "api.go"})
	_ = s.AddFact(meb.Fact{Subject: "api.go", Predicate: "calls", Object: "db.go"})
	_ = s.AddFact(meb.Fact{Subject: "main.go", Predicate: "imports", Object: "pkg/log"})

	// Scan only "calls"
	g, err := NewFromStore(ctx, s, "calls")
	if err != nil {
		t.Fatalf("NewFromStore failed: %v", err)
	}

	if g.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes (main.go, api.go, db.go), got %d", g.NodeCount())
	}
	if g.EdgeCount() != 2 {
		t.Fatalf("expected 2 calls edges, got %d", g.EdgeCount())
	}

	mainID, found := g.LookupID("main.go")
	if !found {
		t.Fatalf("main.go ID not found")
	}
	dbID, found := g.LookupID("db.go")
	if !found {
		t.Fatalf("db.go ID not found")
	}

	if !g.FindReachable(mainID, dbID, 3) {
		t.Fatalf("expected db.go reachable from main.go")
	}
}
