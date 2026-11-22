package meb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynguyendang/meb/circuit"
	"github.com/duynguyendang/meb/query"
)

func TestDatalogQuery_BasicAndConstraints(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Insert test facts
	facts := []Fact{
		{Subject: "main.go", Predicate: "calls", Object: "util.go"},
		{Subject: "main.go", Predicate: "calls", Object: "api.go"},
		{Subject: "util.go", Predicate: "calls", Object: "helper.go"},
		{Subject: "main.go", Predicate: "lines", Object: 150},
		{Subject: "util.go", Predicate: "lines", Object: 45},
		{Subject: "api.go", Predicate: "lines", Object: 300},
	}
	for _, f := range facts {
		if err := store.AddFact(f); err != nil {
			t.Fatalf("AddFact failed: %v", err)
		}
	}

	t.Run("SingleAtomQuery", func(t *testing.T) {
		res, err := store.Query(ctx, `triples(?s, "calls", ?o)`)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(res) != 3 {
			t.Fatalf("expected 3 results, got %d: %v", len(res), res)
		}
	})

	t.Run("SyntacticSugarInequality", func(t *testing.T) {
		res, err := store.Query(ctx, `triples(?s, "calls", ?o), ?o != "util.go"`)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(res) != 2 {
			t.Fatalf("expected 2 results, got %d", len(res))
		}
		for _, row := range res {
			if row["?o"] == "util.go" {
				t.Errorf("found excluded util.go: %v", row)
			}
		}
	})

	t.Run("NumericComparison", func(t *testing.T) {
		res, err := store.Query(ctx, `triples(?s, "lines", ?lines), gt(?lines, 100)`)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(res) != 2 {
			t.Fatalf("expected 2 results > 100, got %d: %v", len(res), res)
		}
	})

	t.Run("StringContainsAndRegex", func(t *testing.T) {
		res, err := store.Query(ctx, `triples(?s, "calls", ?o), contains(?o, "help")`)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(res) != 1 || res[0]["?o"] != "helper.go" {
			t.Fatalf("expected helper.go, got: %v", res)
		}

		resReg, err := store.Query(ctx, `triples(?s, "calls", ?o), regex(?o, "^a.*\.go$")`)
		if err != nil {
			t.Fatalf("regex query failed: %v", err)
		}
		if len(resReg) != 1 || resReg[0]["?o"] != "api.go" {
			t.Fatalf("expected api.go, got: %v", resReg)
		}
	})

	t.Run("NegationNotTriples", func(t *testing.T) {
		// main.go calls util.go, main.go calls api.go, but main.go does NOT call helper.go directly
		res, err := store.Query(ctx, `triples(?s, "calls", ?mid), not(triples(?s, "calls", "helper.go"))`)
		if err != nil {
			t.Fatalf("negation query failed: %v", err)
		}
		if len(res) != 2 { // the 2 main.go calls
			t.Fatalf("expected 2 results, got %d: %v", len(res), res)
		}
	})

	t.Run("JoinLFTJ", func(t *testing.T) {
		res, err := store.Query(ctx, `triples(?a, "calls", ?b), triples(?b, "calls", ?c)`)
		if err != nil {
			t.Fatalf("join query failed: %v", err)
		}
		if len(res) != 1 {
			t.Fatalf("expected 1 join result, got %d: %v", len(res), res)
		}
		if res[0]["?a"] != "main.go" || res[0]["?b"] != "util.go" || res[0]["?c"] != "helper.go" {
			t.Errorf("unexpected join result: %v", res[0])
		}
	})

	t.Run("QueryCache", func(t *testing.T) {
		cache := query.NewQueryCache(10*time.Second, 100, true)
		q := `triples(?s, "calls", ?o)`

		res1, err := store.Query(ctx, q, WithCache(cache))
		if err != nil {
			t.Fatal(err)
		}

		res2, err := store.Query(ctx, q, WithCache(cache))
		if err != nil {
			t.Fatal(err)
		}

		if len(res1) != len(res2) {
			t.Fatalf("cache result count mismatch")
		}
	})
}

func TestDatalogQuery_PropagatesContextError(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddFact(Fact{Subject: "a", Predicate: "calls", Object: "b"}); err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.Query(ctx, `triples(?s, "calls", ?o)`)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestDatalogQuery_CacheIsolatesTopicsAndLimits(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	store.SetTopicID(1)
	if err := store.AddFactBatch([]Fact{
		{Subject: "t1_a", Predicate: "calls", Object: "t1_b"},
		{Subject: "t1_c", Predicate: "calls", Object: "t1_d"},
	}); err != nil {
		t.Fatalf("AddFactBatch topic 1: %v", err)
	}

	store.SetTopicID(2)
	if err := store.AddFact(Fact{Subject: "t2_a", Predicate: "calls", Object: "t2_b"}); err != nil {
		t.Fatalf("AddFact topic 2: %v", err)
	}

	cache := query.NewQueryCache(10*time.Second, 100, true)
	q := `triples(?s, "calls", ?o)`

	store.SetTopicID(1)
	resLimit1, err := store.Query(ctx, q, WithCache(cache), WithLimit(1))
	if err != nil {
		t.Fatalf("Query topic 1 limit 1 failed: %v", err)
	}
	if len(resLimit1) != 1 {
		t.Fatalf("expected 1 result with limit 1, got %d", len(resLimit1))
	}

	// Same query with higher limit must not be truncated by the cached limit-1 entry.
	resLimit2, err := store.Query(ctx, q, WithCache(cache), WithLimit(2))
	if err != nil {
		t.Fatalf("Query topic 1 limit 2 failed: %v", err)
	}
	if len(resLimit2) != 2 {
		t.Fatalf("expected 2 results with limit 2, got %d", len(resLimit2))
	}

	// Same query on topic 2 must not return cached topic 1 results.
	store.SetTopicID(2)
	resT2, err := store.Query(ctx, q, WithCache(cache), WithLimit(10))
	if err != nil {
		t.Fatalf("Query topic 2 failed: %v", err)
	}
	if len(resT2) != 1 || resT2[0]["?s"] != "t2_a" {
		t.Fatalf("expected topic 2 result t2_a, got %v", resT2)
	}
}

func TestDatalogQuery_LimitAppliedAfterConstraints(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	facts := []Fact{
		{Subject: "a", Predicate: "lines", Object: 10},
		{Subject: "b", Predicate: "lines", Object: 20},
		{Subject: "c", Predicate: "lines", Object: 200},
		{Subject: "d", Predicate: "lines", Object: 300},
		{Subject: "e", Predicate: "lines", Object: 400},
	}
	if err := store.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	res, err := store.Query(ctx, `triples(?s, "lines", ?l), gt(?l, 100)`, WithLimit(2))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 results matching gt(?l, 100) with limit 2, got %d: %v", len(res), res)
	}
	for _, r := range res {
		if r["?s"] == "a" || r["?s"] == "b" {
			t.Errorf("unexpected non-matching row in constrained query: %v", r)
		}
	}
}

func TestDatalogQuery_RespectsCircuitBreaker(t *testing.T) {
	store := newTestStore(t)
	if err := store.AddFact(Fact{Subject: "a", Predicate: "calls", Object: "b"}); err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	store.CircuitBreaker().ForceOpen()

	_, err := store.Query(context.Background(), `triples(?s, "calls", ?o)`)
	if !errors.Is(err, circuit.ErrCircuitOpen) {
		t.Fatalf("expected circuit.ErrCircuitOpen, got %v", err)
	}
}
