package vector

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

func TestDefaultHNSWConfig(t *testing.T) {
	cfg := DefaultHNSWConfig()
	if cfg.M != 16 {
		t.Errorf("M = %d, want 16", cfg.M)
	}
	if cfg.MMax != 32 {
		t.Errorf("MMax = %d, want 32", cfg.MMax)
	}
	if cfg.EfConstruction != 200 {
		t.Errorf("EfConstruction = %d, want 200", cfg.EfConstruction)
	}
	if cfg.EfSearch != 64 {
		t.Errorf("EfSearch = %d, want 64", cfg.EfSearch)
	}
}

func TestRandomLevel(t *testing.T) {
	idx := NewHNSWIndex(nil, &HNSWConfig{
		ML: 1.0 / math.Log(16.0),
	}, 128)

	levels := make(map[int]int)
	for i := 0; i < 10000; i++ {
		l := idx.randomLevel()
		levels[l]++
	}

	if len(levels) == 0 {
		t.Fatal("no levels generated")
	}

	total := 0
	for _, count := range levels {
		total += count
	}
	if total != 10000 {
		t.Errorf("expected 10000 samples, got %d", total)
	}

	if levels[0] < 5000 {
		t.Error("expected most nodes at level 0")
	}
}

func TestSelectNeighbors(t *testing.T) {
	idx := NewHNSWIndex(nil, &HNSWConfig{M: 5, MMax: 10}, 128)
	candidates := []uint64{1, 2, 3}
	result := idx.selectNeighbors(candidates, 5, 0)
	if len(result) != 3 {
		t.Errorf("expected 3 neighbors, got %d", len(result))
	}

	candidates2 := []uint64{1, 2, 3, 4, 5, 6, 7}
	result2 := idx.selectNeighbors(candidates2, 5, 0)
	if len(result2) != 5 {
		t.Errorf("expected 5 neighbors, got %d", len(result2))
	}
}

func TestMinHeap2(t *testing.T) {
	h := newMinHeap2(10)
	h.push(nodeDist{id: 3, dist: 3})
	h.push(nodeDist{id: 1, dist: 1})
	h.push(nodeDist{id: 2, dist: 2})

	if h.Len() != 3 {
		t.Fatalf("expected len 3, got %d", h.Len())
	}

	popped := h.pop()
	if popped.id != 1 || popped.dist != 1 {
		t.Errorf("expected {1,1}, got {%d,%f}", popped.id, popped.dist)
	}
}

func TestMaxHeap(t *testing.T) {
	h := newMaxHeap(10)
	h.push(nodeDist{id: 1, dist: 1})
	h.push(nodeDist{id: 3, dist: 3})
	h.push(nodeDist{id: 2, dist: 2})

	if h.Len() != 3 {
		t.Fatalf("expected len 3, got %d", h.Len())
	}

	peeked := h.peek()
	if peeked.id != 3 || peeked.dist != 3 {
		t.Errorf("expected peek {3,3}, got {%d,%f}", peeked.id, peeked.dist)
	}
}

func TestADCAccumScalar(t *testing.T) {
	lut := make([]float32, 2*256)
	lut[0*256+1] = 1.5
	lut[1*256+2] = 2.5
	codes := []byte{1, 2}

	dist := adcAccumScalar(lut, codes)
	expected := float32(4.0)
	if dist != expected {
		t.Errorf("adcAccumScalar = %f, want %f", dist, expected)
	}
}

func newInMemoryHNSWBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestHNSWInsertSoftDeleteAndCompact(t *testing.T) {
	db := newInMemoryHNSWBadger(t)
	cfg := DefaultHNSWConfig()
	idx := NewHNSWIndex(db, cfg, 8)
	ctx := context.Background()
	const topicID uint32 = 1

	for i := uint64(1); i <= 10; i++ {
		vec := make([]float32, 8)
		vec[0] = float32(i)
		if err := idx.Insert(ctx, topicID, i, vec); err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
	}

	ep, ok := idx.GetEntryPoint(topicID)
	if !ok {
		t.Fatal("expected entry point to be set")
	}

	// Delete the entry point plus two other nodes (1, 5, and ep).
	deletedSet := map[uint64]bool{
		ep:                   true,
		keys.PackID(topicID, 2): true,
		keys.PackID(topicID, 5): true,
	}
	for id := range deletedSet {
		if err := idx.SoftDelete(id); err != nil {
			t.Fatalf("SoftDelete(%d) failed: %v", id, err)
		}
	}

	// Compact must prune tombstoned nodes, tombstone keys, dangling neighbor links,
	// re-select a valid entry point, and update nodeCounts/vecCache.
	if err := idx.Compact(ctx, topicID); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	newEP, hasEP := idx.GetEntryPoint(topicID)
	if !hasEP {
		t.Fatal("expected a surviving entry point after Compact")
	}
	if deletedSet[newEP] {
		t.Errorf("entry point %d was deleted and should have been replaced", newEP)
	}

	// Verify no surviving node references any deleted ID in its neighbor lists.
	for i := uint64(1); i <= 10; i++ {
		pid := keys.PackID(topicID, i)
		if deletedSet[pid] {
			continue
		}
		lvl, err := idx.ReadLevel(pid)
		if err != nil {
			t.Fatalf("ReadLevel(%d) failed: %v", i, err)
		}
		neighbors, err := idx.ReadNeighbors(pid, lvl)
		if err != nil {
			t.Fatalf("ReadNeighbors(%d) failed: %v", i, err)
		}
		for _, nid := range neighbors {
			if deletedSet[nid] {
				t.Errorf("surviving node %d still has dangling neighbor %d after Compact", pid, nid)
			}
		}
	}

	// Verify tombstones and deleted HNSW keys are removed from BadgerDB.
	err := db.View(func(txn *badger.Txn) error {
		for id := range deletedSet {
			if _, err := txn.Get(keys.EncodeHNSWTombstoneKey(id)); err != badger.ErrKeyNotFound {
				return fmt.Errorf("tombstone key for %d still exists (err=%v)", id, err)
			}
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			pfx := keys.HNSWNodePrefix(id)
			it.Seek(pfx)
			if it.ValidForPrefix(pfx) {
				it.Close()
				return fmt.Errorf("HNSW node key for deleted id %d still exists", id)
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify SearchInTopic returns only surviving nodes.
	q := make([]float32, 8)
	q[0] = 6.0
	var results []SearchResult
	for res, err := range idx.SearchInTopic(ctx, topicID, q, 10) {
		if err != nil {
			t.Fatalf("SearchInTopic failed: %v", err)
		}
		if deletedSet[res.ID] {
			t.Errorf("SearchInTopic returned deleted node %d", res.ID)
		}
		results = append(results, res)
	}
	if len(results) == 0 {
		t.Fatal("expected surviving results after Compact")
	}

	idx.nodeCountMu.RLock()
	cnt := idx.nodeCounts[topicID]
	idx.nodeCountMu.RUnlock()
	wantCount := 10 - len(deletedSet)
	if cnt != wantCount {
		t.Errorf("nodeCounts[%d] = %d, want %d", topicID, cnt, wantCount)
	}
}

func TestHNSWConcurrentInsertAndCompact(t *testing.T) {
	db := newInMemoryHNSWBadger(t)
	idx := NewHNSWIndex(db, DefaultHNSWConfig(), 8)
	ctx := context.Background()
	const topicID uint32 = 1

	for i := uint64(1); i <= 10; i++ {
		vec := make([]float32, 8)
		vec[0] = float32(i)
		if err := idx.Insert(ctx, topicID, i, vec); err != nil {
			t.Fatalf("seed Insert failed: %v", err)
		}
	}
	_ = idx.SoftDelete(keys.PackID(topicID, 2))
	_ = idx.SoftDelete(keys.PackID(topicID, 4))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := uint64(11); i <= 30; i++ {
			vec := make([]float32, 8)
			vec[0] = float32(i)
			_ = idx.Insert(ctx, topicID, i, vec)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_ = idx.Compact(ctx, topicID)
		}
	}()
	wg.Wait()
}

func TestHNSW_IncrementalCompactionAndTombstoneRatio(t *testing.T) {
	db := newInMemoryHNSWBadger(t)
	cfg := DefaultHNSWConfig()
	cfg.CompactionRatio = 0.15
	cfg.CompactionBatchSize = 5
	idx := NewHNSWIndex(db, cfg, 8)
	ctx := context.Background()
	const topicID uint32 = 1

	const totalNodes = 30
	for i := uint64(1); i <= totalNodes; i++ {
		vec := make([]float32, 8)
		vec[0] = float32(i)
		if err := idx.Insert(ctx, topicID, i, vec); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Delete 3 nodes (10% < 15%): ShouldCompact must be false.
	for i := uint64(1); i <= 3; i++ {
		if err := idx.SoftDelete(keys.PackID(topicID, i)); err != nil {
			t.Fatalf("SoftDelete(%d): %v", i, err)
		}
	}
	if idx.ShouldCompact(topicID) {
		t.Fatalf("expected ShouldCompact=false at 10%% tombstone ratio (got ratio=%.2f)", idx.TombstoneRatio(topicID))
	}

	// Delete 3 more nodes (20% >= 15%): ShouldCompact must become true.
	for i := uint64(4); i <= 6; i++ {
		if err := idx.SoftDelete(keys.PackID(topicID, i)); err != nil {
			t.Fatalf("SoftDelete(%d): %v", i, err)
		}
	}
	if !idx.ShouldCompact(topicID) {
		t.Fatalf("expected ShouldCompact=true at 20%% tombstone ratio (got ratio=%.2f)", idx.TombstoneRatio(topicID))
	}

	// Step through CompactIncremental with small batches (5 nodes/tick).
	ticks := 0
	for {
		ticks++
		_, done, err := idx.CompactIncremental(ctx, topicID, 5)
		if err != nil {
			t.Fatalf("CompactIncremental tick %d: %v", ticks, err)
		}
		if done {
			break
		}
		if ticks > 50 {
			t.Fatal("incremental compaction did not terminate")
		}
	}
	if ticks < 2 {
		t.Errorf("expected multiple incremental ticks with batchSize=5, got %d", ticks)
	}
	if idx.TombstoneCount(topicID) != 0 {
		t.Errorf("expected TombstoneCount=0 after full pass, got %d", idx.TombstoneCount(topicID))
	}
	if idx.ShouldCompact(topicID) {
		t.Error("expected ShouldCompact=false after compaction completes")
	}

	idx.nodeCountMu.RLock()
	cnt := idx.nodeCounts[topicID]
	idx.nodeCountMu.RUnlock()
	if cnt != totalNodes-6 {
		t.Errorf("nodeCounts[%d] = %d, want %d", topicID, cnt, totalNodes-6)
	}
}
