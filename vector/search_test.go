package vector

import (
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func setupTestDB(t *testing.T) *badger.DB {
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := 0; i < dim; i++ {
		v[i] = rand.Float32()*2 - 1.0
	}
	return v
}

func TestVectorRegistry_Search(t *testing.T) {
	db := setupTestDB(t)
	reg := NewRegistry(db)
	defer reg.Close()

	numVectors := 100
	vectors := make(map[uint64][]float32)

	for i := uint64(1); i <= uint64(numVectors); i++ {
		vec := randomVector(FullDim)
		vectors[i] = vec
		err := reg.Add(i, vec)
		if err != nil {
			t.Fatalf("failed to add vector: %v", err)
		}
	}

	if reg.Count() != numVectors {
		t.Errorf("expected %d vectors, got %d", numVectors, reg.Count())
	}

	// Wait for async persistence (WaitGroup logic in `Add`)
	// Although the scan works completely in memory on the int8 buffers,
	// closing or waiting ensures everything settled. We don't have to wait
	// because `Search` only hits the RAM buffer.

	query := vectors[5] // exact match
	k := 10
	results, err := reg.Search(query, k)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != k {
		t.Errorf("expected %d results, got %d", k, len(results))
	}

	// Vector 5 should be the top hit since it's exactly identical
	if len(results) > 0 && results[0].ID != 5 {
		t.Errorf("expected top hit to be 5, got %d with score %v", results[0].ID, results[0].Score)
	}

	// Test edge cases
	emptyResults, err := reg.Search(query, 0)
	if err != nil || len(emptyResults) != 0 {
		t.Errorf("expected 0 results for k=0")
	}

	largeResults, err := reg.Search(query, numVectors+10)
	if err != nil || len(largeResults) != numVectors {
		t.Errorf("expected %d results when K > numVectors, got %d", numVectors, len(largeResults))
	}
}

func TestGetTopK(t *testing.T) {
	results := []SearchResult{
		{ID: 1, Score: 0.1},
		{ID: 2, Score: 0.9},
		{ID: 3, Score: 0.5},
		{ID: 4, Score: 0.7},
	}

	top3 := getTopK(results, 3)
	if len(top3) != 3 {
		t.Fatalf("expected 3 results, got %d", len(top3))
	}

	if top3[0].ID != 2 || top3[1].ID != 4 || top3[2].ID != 3 {
		t.Errorf("incorrect top K sorting: %v", top3)
	}
}
