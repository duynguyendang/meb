package dict

import (
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func openAllocatorDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRangeAllocator_Basic(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewRangeAllocator(db, 100)
	if err != nil {
		t.Fatalf("NewRangeAllocator: %v", err)
	}

	id1, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if id1 != 1 {
		t.Errorf("expected id 1, got %d", id1)
	}

	id2, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if id2 != 2 {
		t.Errorf("expected id 2, got %d", id2)
	}
}

func TestRangeAllocator_BlockReservation(t *testing.T) {
	db := openAllocatorDB(t)
	blockSize := uint64(5)
	alloc, err := NewRangeAllocator(db, blockSize)
	if err != nil {
		t.Fatalf("NewRangeAllocator: %v", err)
	}

	ids := make([]uint64, 0, 12)
	for i := 0; i < 12; i++ {
		id, err := alloc.Allocate()
		if err != nil {
			t.Fatalf("Allocate %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Errorf("non-contiguous IDs: ids[%d]=%d, ids[%d]=%d", i-1, ids[i-1], i, ids[i])
		}
	}
	if ids[0] != 1 {
		t.Errorf("expected first ID to be 1, got %d", ids[0])
	}
}

func TestRangeAllocator_Persistence(t *testing.T) {
	db := openAllocatorDB(t)
	alloc1, err := NewRangeAllocator(db, 100)
	if err != nil {
		t.Fatalf("NewRangeAllocator: %v", err)
	}

	for i := 0; i < 50; i++ {
		if _, err := alloc1.Allocate(); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
	}

	alloc2, err := NewRangeAllocator(db, 100)
	if err != nil {
		t.Fatalf("NewRangeAllocator (reload): %v", err)
	}

	id, err := alloc2.Allocate()
	if err != nil {
		t.Fatalf("Allocate after reload: %v", err)
	}
	if id <= 50 {
		t.Errorf("expected id > 50 after reload, got %d", id)
	}
}

func TestRangeAllocator_Reset(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewRangeAllocator(db, 100)
	if err != nil {
		t.Fatalf("NewRangeAllocator: %v", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := alloc.Allocate(); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
	}

	if err := alloc.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	id, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate after reset: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id 1 after reset, got %d", id)
	}
}

func TestRangeAllocator_AllocateBatch(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewRangeAllocator(db, 100)
	if err != nil {
		t.Fatalf("NewRangeAllocator: %v", err)
	}

	start, err := alloc.AllocateBatch(10)
	if err != nil {
		t.Fatalf("AllocateBatch: %v", err)
	}
	if start != 1 {
		t.Errorf("expected batch start 1, got %d", start)
	}

	start2, err := alloc.AllocateBatch(5)
	if err != nil {
		t.Fatalf("AllocateBatch: %v", err)
	}
	if start2 != 11 {
		t.Errorf("expected batch start 11, got %d", start2)
	}
}

func TestShardedAllocator_RoundRobin(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewShardedAllocator(db, 1000, 4)
	if err != nil {
		t.Fatalf("NewShardedAllocator: %v", err)
	}

	if alloc.NumShards() != 4 {
		t.Errorf("expected 4 shards, got %d", alloc.NumShards())
	}

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		id, err := alloc.Allocate()
		if err != nil {
			t.Fatalf("Allocate %d: %v", i, err)
		}
		if ids[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		ids[id] = true
	}
}

func TestShardedAllocator_Concurrent(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewShardedAllocator(db, 1000, 4)
	if err != nil {
		t.Fatalf("NewShardedAllocator: %v", err)
	}

	const goroutines = 8
	const perGoroutine = 100

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ids := make([]uint64, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				id, err := alloc.Allocate()
				if err != nil {
					t.Errorf("goroutine %d Allocate: %v", idx, err)
					return
				}
				ids = append(ids, id)
			}
			results[idx] = ids
		}(g)
	}

	wg.Wait()

	seen := make(map[uint64]bool)
	for g, ids := range results {
		for _, id := range ids {
			if seen[id] {
				t.Errorf("duplicate ID %d from goroutine %d", id, g)
			}
			seen[id] = true
		}
	}

	if len(seen) != goroutines*perGoroutine {
		t.Errorf("expected %d unique IDs, got %d", goroutines*perGoroutine, len(seen))
	}
}

func TestShardedAllocator_SingleShard(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewShardedAllocator(db, 100, 1)
	if err != nil {
		t.Fatalf("NewShardedAllocator: %v", err)
	}

	if alloc.NumShards() != 1 {
		t.Errorf("expected 1 shard, got %d", alloc.NumShards())
	}

	id, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}
}

func TestShardedAllocator_Reset(t *testing.T) {
	db := openAllocatorDB(t)
	alloc, err := NewShardedAllocator(db, 100, 4)
	if err != nil {
		t.Fatalf("NewShardedAllocator: %v", err)
	}

	for i := 0; i < 50; i++ {
		if _, err := alloc.Allocate(); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
	}

	if err := alloc.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	id, err := alloc.Allocate()
	if err != nil {
		t.Fatalf("Allocate after reset: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id 1 after reset, got %d", id)
	}
}
