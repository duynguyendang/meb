package meb

import (
	"context"
	"strings"
	"testing"

	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/factstore"
	"codeberg.org/TauCeti/mangle-go/provenance"
)

func TestMangleFactStore_WithRemoveAndTypedConstants(t *testing.T) {
	store := newTestStore(t)
	fs := store.FactStore()

	// Verify FactStoreWithRemove interface satisfaction.
	var _ factstore.FactStoreWithRemove = fs

	a1 := ast.NewAtom("triples", ast.String("fnA"), ast.String("calls"), ast.String("fnB"))
	a2 := ast.NewAtom("triples", ast.String("fnA"), ast.String("lines"), ast.Number(42))
	a3 := ast.NewAtom("derived_flag", ast.String("fnA"), ast.Number(1))

	if !fs.Add(a1) {
		t.Fatal("expected Add(a1) to return true")
	}
	if fs.Add(a1) {
		t.Fatal("expected duplicate Add(a1) to return false")
	}
	if !fs.Add(a2) {
		t.Fatal("expected Add(a2) with Number constant to return true")
	}
	if !fs.Add(a3) {
		t.Fatal("expected Add(a3) to overlay to return true")
	}

	if !fs.Contains(a1) {
		t.Fatal("expected Contains(a1) to be true")
	}
	if !fs.Contains(a2) {
		t.Fatal("expected Contains(a2) with Number constant to be true")
	}
	if !fs.Contains(a3) {
		t.Fatal("expected Contains(a3) to be true")
	}

	if fs.EstimateFactCount() < 3 {
		t.Errorf("expected EstimateFactCount >= 3, got %d", fs.EstimateFactCount())
	}

	if !fs.Remove(a1) {
		t.Fatal("expected Remove(a1) to return true")
	}
	if fs.Contains(a1) {
		t.Fatal("expected Contains(a1) to be false after Remove")
	}
	if !fs.Remove(a3) {
		t.Fatal("expected Remove(a3) from overlay to return true")
	}
	if fs.Contains(a3) {
		t.Fatal("expected Contains(a3) to be false after Remove")
	}
}

func TestEvalMangle_V07FeaturesAndBuiltins(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.AddFactBatch([]Fact{
		{Subject: "authHandler", Predicate: "calls", Object: "userRepo"},
		{Subject: "userRepo", Predicate: "calls", Object: "dbConn"},
		{Subject: "authHandler", Predicate: "latency", Object: float32(12.5)},
		{Subject: "userRepo", Predicate: "latency", Object: float32(1.5)},
	}); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	// Exercises Mangle v0.7 features:
	// - Recursive rules over MEBStore triples
	// - :string:matches (new in v0.7)
	// - :float:gt (new in v0.7)
	// - fn:max over a list literal (v0.7 #103 regression fix)
	// - :filter(...) type-checked predicate (v0.7)
	// - ListMember with bound element (ArgModeInputOutput in v0.7)
	program := `
		calls(A, B) :- triples(A, "calls", B).
		reaches(A, B) :- calls(A, B).
		reaches(A, C) :- calls(A, B), reaches(B, C).

		handler_node(S) :-
			triples(S, "calls", _),
			:string:matches(S, ".*Handler$").

		slow_node(S) :-
			triples(S, "latency", L),
			:float:gt(L, 5.0).

		list_check(M) :-
			M = fn:max([1, 7, 3]),
			:filter(fn:list:contains([1, 7, 3], 7)).

		has_member(X) :-
			X = "dbConn",
			:list:member(X, ["userRepo", "dbConn"]).
	`

	res, err := store.EvalMangle(ctx, program)
	if err != nil {
		t.Fatalf("EvalMangle failed: %v", err)
	}

	reaches, err := res.QueryAtoms(`reaches("authHandler", X)`)
	if err != nil {
		t.Fatalf("QueryAtoms reaches: %v", err)
	}
	if len(reaches) != 2 {
		t.Fatalf("expected 2 reachable nodes from authHandler, got %d: %v", len(reaches), reaches)
	}

	handlers, err := res.QueryAtoms(`handler_node(S)`)
	if err != nil {
		t.Fatalf("QueryAtoms handler_node: %v", err)
	}
	if len(handlers) != 1 || handlers[0].Args[0].(ast.Constant).Symbol != "authHandler" {
		t.Fatalf("expected [authHandler], got %v", handlers)
	}

	slow, err := res.QueryAtoms(`slow_node(S)`)
	if err != nil {
		t.Fatalf("QueryAtoms slow_node: %v", err)
	}
	if len(slow) != 1 || slow[0].Args[0].(ast.Constant).Symbol != "authHandler" {
		t.Fatalf("expected [authHandler] for slow_node, got %v", slow)
	}

	listChecks, err := res.QueryAtoms(`list_check(M)`)
	if err != nil {
		t.Fatalf("QueryAtoms list_check: %v", err)
	}
	if len(listChecks) != 1 || listChecks[0].Args[0].(ast.Constant).NumValue != 7 {
		t.Fatalf("expected list_check(7), got %v", listChecks)
	}

	members, err := res.QueryAtoms(`has_member(X)`)
	if err != nil {
		t.Fatalf("QueryAtoms has_member: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 has_member result, got %v", members)
	}
}

func TestExplainMangle_FullProvenanceAndAggregation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.AddFactBatch([]Fact{
		{Subject: "a.go", Predicate: "calls", Object: "b.go"},
		{Subject: "b.go", Predicate: "calls", Object: "c.go"},
	}); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	program := `
		calls(A, B) :- triples(A, "calls", B).
		reaches(A, B) :- calls(A, B).
		reaches(A, C) :- calls(A, B), reaches(B, C).
		out_degree(A, N) :- calls(A, B) |> do fn:group_by(A), let N = fn:count().
	`

	proofs, formatted, err := store.ExplainMangle(ctx, program, `reaches("a.go", "c.go")`)
	if err != nil {
		t.Fatalf("ExplainMangle failed: %v", err)
	}
	if len(proofs) != 1 {
		t.Fatalf("expected 1 proof, got %d", len(proofs))
	}
	if proofs[0].Kind != provenance.KindDerived {
		t.Errorf("expected KindDerived, got %v", proofs[0].Kind)
	}
	if !strings.Contains(formatted, `reaches("a.go","c.go")`) {
		t.Errorf("unexpected formatted proof:\n%s", formatted)
	}

	// Full provenance over do-aggregation (requires v0.7 DerivationRecorder).
	aggProofs, aggText, err := store.ExplainMangle(ctx, program, `out_degree("a.go", 1)`)
	if err != nil {
		t.Fatalf("ExplainMangle aggregation failed: %v", err)
	}
	if len(aggProofs) != 1 || aggProofs[0].Kind != provenance.KindDoAggregate {
		t.Fatalf("expected KindDoAggregate proof, got %+v", aggProofs)
	}
	if !strings.Contains(aggText, "do fn:group_by") {
		t.Errorf("expected aggregation transform in proof text:\n%s", aggText)
	}

	// Emit proof DAG as queryable Mangle facts.
	proofStore, _, err := store.ExplainMangleAsFacts(ctx, program, `reaches("a.go", "c.go")`)
	if err != nil {
		t.Fatalf("ExplainMangleAsFacts failed: %v", err)
	}
	var edbLeaves []ast.Atom
	if err := proofStore.GetFacts(ast.NewQuery(ast.PredicateSym{Symbol: "edb_leaf", Arity: 2}), func(a ast.Atom) error {
		edbLeaves = append(edbLeaves, a)
		return nil
	}); err != nil {
		t.Fatalf("GetFacts edb_leaf: %v", err)
	}
	if len(edbLeaves) != 2 {
		t.Errorf("expected 2 edb_leaf provenance facts, got %d", len(edbLeaves))
	}
}
