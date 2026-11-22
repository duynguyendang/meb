package vector

import (
	"context"
	"testing"

	"go.uber.org/goleak"
)

func TestMmapSegmentLRUEvictionAndColdFallback(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	db := newInMemoryVectorBadger(t)
	segDir := t.TempDir()

	// Determine vectorSize first for deterministic small segment geometry:
	// 1 vector per segment so inserting 4 vectors creates 4 distinct segments.
	probe := NewRegistry(db, &Config{
		FullDim:         32,
		HybridBitWidth:  8,
		HybridBlockSize: 32,
		NumWorkers:      2,
		VectorCapacity:  16,
		InitialCapacity: 16,
		SegmentDir:      segDir,
		SegmentSize:     1, // clamps vectorsPerSegment to 1
	})
	vSize := probe.VectorSize()
	_ = probe.Close()

	maxBudget := int64(vSize * 2) // room for at most 2 mapped segments
	r := NewRegistry(db, &Config{
		FullDim:              32,
		HybridBitWidth:       8,
		HybridBlockSize:      32,
		NumWorkers:           2,
		VectorCapacity:       16,
		InitialCapacity:      16,
		SegmentDir:           segDir,
		SegmentSize:          vSize,
		MaxMmapBytes:         maxBudget,
		AutoPromoteThreshold: -1, // keep in brute-force / Badger fallback mode
	})
	t.Cleanup(func() { _ = r.Close() })

	// Insert 4 distinct vectors across 4 segments.
	targetVec := make([]float32, 32)
	targetVec[0] = 1

	v0 := make([]float32, 32)
	v0[1] = 1
	v1 := make([]float32, 32)
	v1[2] = 1
	v3 := make([]float32, 32)
	v3[3] = 1

	// Touch segment 0 before adding segment 2 & 3 so LRU respects access recency.
	if err := r.Add(1, v0); err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	if err := r.Add(2, v1); err != nil {
		t.Fatalf("Add 2: %v", err)
	}
	r.TouchSegment(0) // segment 1 becomes oldest

	if err := r.Add(3, targetVec); err != nil {
		t.Fatalf("Add 3: %v", err)
	}
	if err := r.Add(4, v3); err != nil {
		t.Fatalf("Add 4: %v", err)
	}

	if got := r.MappedBytes(); got > maxBudget {
		t.Errorf("MappedBytes() = %d, want <= %d", got, maxBudget)
	}
	if evicted := r.EvictedSegmentCount(); evicted < 2 {
		t.Errorf("EvictedSegmentCount() = %d, want >= 2", evicted)
	}

	// Search must transparently fall back to Badger 0x11 streaming and still find targetVec (id=3)
	// as top-1 even though segments were unmapped.
	var results []SearchResult
	for res, err := range r.Search(context.Background(), targetVec, 4) {
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		results = append(results, res)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results from cold Badger fallback, got %d", len(results))
	}
	if results[0].ID != 3 {
		t.Errorf("expected ID 3 as top-1 match, got %d (score=%.4f)", results[0].ID, results[0].Score)
	}
}
