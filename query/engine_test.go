package query

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"
)

func TestExecuteStream_Basic(t *testing.T) {
	vecResults := func(yield func(StreamResult, error) bool) {
		yield(StreamResult{ID: 1, Score: 0.9}, nil)
		yield(StreamResult{ID: 2, Score: 0.8}, nil)
	}
	lftjResults := func(yield func(StreamResult, error) bool) {
		yield(StreamResult{ID: 3, Score: 0.7}, nil)
		yield(StreamResult{ID: 4, Score: 0.6}, nil)
	}

	var results []StreamResult
	for r, err := range ExecuteStream(context.Background(), vecResults, lftjResults, 10) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 4 {
		t.Errorf("expected 4 results, got %d", len(results))
	}
}

func TestExecuteStream_Limit(t *testing.T) {
	vecResults := func(yield func(StreamResult, error) bool) {
		for i := 0; i < 100; i++ {
			if !yield(StreamResult{ID: uint64(i), Score: 1.0}, nil) {
				return
			}
		}
	}
	lftjResults := func(yield func(StreamResult, error) bool) {}

	count := 0
	for _, err := range ExecuteStream(context.Background(), vecResults, lftjResults, 5) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count > 5 {
		t.Errorf("expected at most 5 results, got %d", count)
	}
}

func TestExecuteStream_EarlyExit(t *testing.T) {
	vecResults := func(yield func(StreamResult, error) bool) {
		for i := 0; i < 1000; i++ {
			if !yield(StreamResult{ID: uint64(i), Score: 1.0}, nil) {
				return
			}
		}
	}
	lftjResults := func(yield func(StreamResult, error) bool) {
		for i := 1000; i < 2000; i++ {
			if !yield(StreamResult{ID: uint64(i), Score: 1.0}, nil) {
				return
			}
		}
	}

	count := 0
	for _, err := range ExecuteStream(context.Background(), vecResults, lftjResults, 100) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
		if count >= 10 {
			break
		}
	}
}

func TestExecuteStream_Error(t *testing.T) {
	testErr := errors.New("test error")
	vecResults := func(yield func(StreamResult, error) bool) {
		yield(StreamResult{}, testErr)
	}
	lftjResults := func(yield func(StreamResult, error) bool) {}

	var gotErr error
	for _, err := range ExecuteStream(context.Background(), vecResults, lftjResults, 10) {
		if err != nil {
			gotErr = err
		}
	}

	if gotErr == nil {
		t.Fatal("expected error from stream")
	}
	if !errors.Is(gotErr, testErr) {
		t.Errorf("expected test error, got %v", gotErr)
	}
}

func TestExecuteStream_BothErrors(t *testing.T) {
	err1 := errors.New("vec error")
	err2 := errors.New("lftj error")
	vecResults := func(yield func(StreamResult, error) bool) {
		yield(StreamResult{}, err1)
	}
	lftjResults := func(yield func(StreamResult, error) bool) {
		yield(StreamResult{}, err2)
	}

	var errs []error
	for _, err := range ExecuteStream(context.Background(), vecResults, lftjResults, 10) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		t.Fatal("expected errors from stream")
	}
	joined := errs[0]
	if !errors.Is(joined, err1) && !errors.Is(joined, err2) {
		t.Errorf("expected joined error to contain both errors, got %v", joined)
	}
}

func TestExecuteWithSeeds_MultipleSeeds(t *testing.T) {
	db := newTestDB(t)

	for i := uint64(1); i <= 3; i++ {
		insertTriple(t, db, 0x20, i, 100, i+10)
		insertTriple(t, db, 0x21, i+10, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(1000000)

	seeds := func(yield func(uint64, error) bool) {
		yield(1, nil)
		yield(2, nil)
		yield(3, nil)
	}

	relations := []RelationPattern{
		{
			Prefix:            0x20,
			BoundPositions:    map[int]uint64{0: 0},
			VariablePositions: map[int]string{0: "s", 2: "o"},
		},
	}

	count := 0
	for _, err := range engine.ExecuteWithSeeds(ctx, relations, "s", seeds, []string{"s", "o"}) {
		if err != nil {
			t.Fatalf("ExecuteWithSeeds error: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 results (one per seed), got %d", count)
	}
}

func TestQueryPlanCache_HitMiss(t *testing.T) {
	cache := NewQueryPlanCache(10, 5*time.Minute)

	relations := []RelationPattern{
		{Prefix: 0x20, BoundPositions: map[int]uint64{0: 1}, VariablePositions: map[int]string{2: "o"}},
	}
	resultVars := []string{"s", "o"}
	key := planKey(relations, resultVars)

	_, _, ok := cache.Get(key)
	if ok {
		t.Fatal("expected cache miss on first lookup")
	}

	cache.Set(key, relations, 100)

	entry, count, ok := cache.Get(key)
	if !ok {
		t.Fatal("expected cache hit after Set")
	}
	if count != 100 {
		t.Errorf("expected estimatedCount 100, got %d", count)
	}
	if len(entry) != 1 {
		t.Errorf("expected 1 relation, got %d", len(entry))
	}
}

func TestQueryPlanCache_Eviction(t *testing.T) {
	cache := NewQueryPlanCache(3, 5*time.Minute)

	for i := 0; i < 5; i++ {
		relations := []RelationPattern{
			{Prefix: byte(i), BoundPositions: map[int]uint64{0: uint64(i)}},
		}
		key := planKey(relations, nil)
		cache.Set(key, relations, i*10)
	}

	_, _, size, _ := cache.Stats()
	if size > 3 {
		t.Errorf("expected at most 3 entries after eviction, got %d", size)
	}
}

func TestQueryPlanCache_Expiry(t *testing.T) {
	cache := NewQueryPlanCache(10, 10*time.Millisecond)

	relations := []RelationPattern{
		{Prefix: 0x20, BoundPositions: map[int]uint64{0: 1}},
	}
	key := planKey(relations, nil)
	cache.Set(key, relations, 50)

	_, _, ok := cache.Get(key)
	if !ok {
		t.Fatal("expected cache hit immediately after Set")
	}

	time.Sleep(20 * time.Millisecond)

	_, _, ok = cache.Get(key)
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestQueryPlanCache_Stats(t *testing.T) {
	cache := NewQueryPlanCache(10, 5*time.Minute)

	relations := []RelationPattern{
		{Prefix: 0x20, BoundPositions: map[int]uint64{0: 1}},
	}
	key := planKey(relations, nil)

	cache.Get(key)
	cache.Get(key)
	cache.Set(key, relations, 50)
	cache.Get(key)
	cache.Get(key)

	hits, misses, _, _ := cache.Stats()
	if hits != 2 {
		t.Errorf("expected 2 hits, got %d", hits)
	}
	if misses != 2 {
		t.Errorf("expected 2 misses, got %d", misses)
	}
}

func makeSeedSeq(ids ...uint64) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		for _, id := range ids {
			if !yield(id, nil) {
				return
			}
		}
	}
}
