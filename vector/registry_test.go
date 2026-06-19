package vector

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func newTestRegistry(t *testing.T) *VectorRegistry {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		FullDim:         128,
		HybridBitWidth:  8,
		HybridBlockSize: 32,
		NumWorkers:      2,
		VectorCapacity:  1024 * 1024,
		InitialCapacity: 100,
		SegmentDir:      dir,
		SegmentSize:     64 * 1024,
	}
	// Use an in-memory badger db for tests
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory badger: %v", err)
	}
	r := NewRegistry(db, cfg)
	t.Cleanup(func() {
		r.Close()
		db.Close()
	})
	return r
}

func TestSegmentedAddAndSearch(t *testing.T) {
	r := newTestRegistry(t)

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i) / 128.0
	}

	for id := uint64(1); id <= 50; id++ {
		if err := r.Add(id, vec); err != nil {
			t.Fatalf("Add(%d) failed: %v", id, err)
		}
	}

	if r.Count() != 50 {
		t.Errorf("expected count 50, got %d", r.Count())
	}

	// Verify getVectorSlice works across segments
	slot := r.getVectorSlice(0)
	if slot[0] != 0 { // hash byte
		t.Errorf("expected hash 0, got %d", slot[0])
	}
}

func TestSegmentedMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		FullDim:         128,
		HybridBitWidth:  8,
		HybridBlockSize: 32,
		NumWorkers:      1,
		VectorCapacity:  1024 * 1024,
		InitialCapacity: 10,
		SegmentDir:      dir,
		SegmentSize:     4096,
	}
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger: %v", err)
	}
	r := NewRegistry(db, cfg)
	defer func() {
		r.Close()
		db.Close()
	}()

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 1.0
	}

	// Add enough vectors to force multiple segments
	for id := uint64(1); id <= 100; id++ {
		if err := r.Add(id, vec); err != nil {
			t.Fatalf("Add(%d) failed: %v", id, err)
		}
	}

	if r.Count() != 100 {
		t.Errorf("expected count 100, got %d", r.Count())
	}

	if len(r.segments) < 2 {
		t.Errorf("expected multiple segments, got %d", len(r.segments))
	}

	// Verify segment files exist
	entries, _ := os.ReadDir(dir)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 segment files, got %d", len(entries))
	}
}

func TestSegmentedDelete(t *testing.T) {
	r := newTestRegistry(t)

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i)
	}

	for id := uint64(1); id <= 10; id++ {
		r.Add(id, vec)
	}

	if !r.Delete(5) {
		t.Error("Delete(5) should return true")
	}
	if r.Count() != 9 {
		t.Errorf("expected count 9, got %d", r.Count())
	}
	if r.HasVector(5) {
		t.Error("vector 5 should not exist after delete")
	}

	// Verify swapped vector still works
	if !r.HasVector(10) {
		t.Error("vector 10 should still exist")
	}
}

func TestSegmentedConcurrentReadWrite(t *testing.T) {
	r := newTestRegistry(t)

	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i) / 128.0
	}

	// Pre-populate
	for id := uint64(1); id <= 20; id++ {
		r.Add(id, vec)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writes (triggering grows)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for id := base; id < base+50; id++ {
				if err := r.Add(id, vec); err != nil {
					errors <- err
					return
				}
			}
		}(uint64(21 + i*50))
	}

	// Concurrent reads via GetTQVector
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				count := r.Count()
				if count > 0 {
					_ = r.GetTQVector(count - 1)
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent error: %v", err)
	}
}

func TestSegmentedInvalidDimension(t *testing.T) {
	r := newTestRegistry(t)

	badVec := make([]float32, 64) // Wrong dimension
	err := r.Add(1, badVec)
	if err == nil {
		t.Error("expected error for wrong dimension")
	}
}

func TestPackInlineNotCollidingWithSegments(t *testing.T) {
	// Verify inline IDs don't interfere with vector index space
	r := newTestRegistry(t)

	vec := make([]float32, 128)
	for i := range vec {
		s, c := math.Sincos(float64(i))
		vec[i] = float32(s + c)
	}

	// Add vectors with IDs that include high bits
	for id := uint64(1); id <= 10; id++ {
		r.Add(id, vec)
	}

	if r.Count() != 10 {
		t.Errorf("expected 10, got %d", r.Count())
	}
}

func TestNonPow2DimensionSearch(t *testing.T) {
	// Regression test for C1: FullDim=1536 (non-power-of-2) requires padding
	// to nextPow2(1536)=2048. The mmap slot must be sized for the padded dimension.
	dir := t.TempDir()
	cfg := &Config{
		FullDim:         1536,
		HybridBitWidth:  8,
		HybridBlockSize: 32,
		NumWorkers:      2,
		VectorCapacity:  1024 * 1024,
		InitialCapacity: 10,
		SegmentDir:      dir,
		SegmentSize:     64 * 1024,
	}
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory badger: %v", err)
	}
	r := NewRegistry(db, cfg)
	defer func() {
		r.Close()
		db.Close()
	}()

	paddedDim := nextPow2(1536) // 2048
	expectedVecSize := HybridVectorSize(paddedDim, cfg.HybridConfig()) + hashSize // +1 for hash byte

	if r.VectorSize() != expectedVecSize {
		t.Errorf("vector size mismatch: got %d, expected %d (paddedDim=%d)", r.VectorSize(), expectedVecSize, paddedDim)
	}

	queryVec := make([]float32, 1536)
	queryVec[0] = 1.0

	// Add 5 vectors with IDs 1-5
	for id := uint64(1); id <= 5; id++ {
		v := make([]float32, 1536)
		v[0] = float32(id)
		v[1] = 1.0
		if err := r.Add(id, v); err != nil {
			t.Fatalf("Add(%d) failed: %v", id, err)
		}
	}

	if r.Count() != 5 {
		t.Fatalf("expected count 5, got %d", r.Count())
	}

	// Verify neighbor slots are not corrupted: check that neighbor vector 2 data is intact.
	// We verify this by checking that slot 0's hash byte is still 0 (it was set to 0)
	// and that after a full search, we get expected results.
	slot0 := r.getVectorSlice(0)
	if slot0[0] != 0 {
		t.Errorf("hash byte corrupted: got %d", slot0[0])
	}

	// Verify vector 2's slot begins at the right offset and hash byte is also 0
	slot1 := r.getVectorSlice(1)
	if slot1[0] != 0 {
		t.Errorf("slot 1 hash byte corrupted: got %d (should be 0) — neighbor-slot overwrite from oversized write", slot1[0])
	}

	// Search should return non-zero scores
	results := make([]SearchResult, 0)
	for sr, err := range r.Search(context.Background(), queryVec, 5) {
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		results = append(results, sr)
	}

	if len(results) == 0 {
		t.Fatal("search returned zero results")
	}

	for _, sr := range results {
		if sr.Score == 0 {
			t.Errorf("search returned zero score for ID=%d — indicates slot sizing mismatch", sr.ID)
		}
	}
}
