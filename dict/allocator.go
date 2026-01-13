package dict

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

const (
	// DefaultBlockSize is the number of IDs to allocate in a single block
	DefaultBlockSize = 10000

	// Key for storing the global counter in BadgerDB
	// Using a system prefix key
	globalCounterKey = "__dict_global_counter"
)

// RangeAllocator handles high-speed parallel ID allocation using range blocks.
// It persists the global counter to disk but allocates IDs from RAM blocks.
type RangeAllocator struct {
	db        *badger.DB
	blockSize uint64

	// Global counter state (protected by mu)
	globalMax uint64     // The highest ID currently persisted/reserved
	mu        sync.Mutex // Protects globalMax and DB writes

	// Local allocation state (atomic)
	// We use a simplified model here: the allocator itself hands out IDs one by one
	// from the CURRENT reserved block. When that block is exhausted, it reserves a new one.
	// This avoids complex worker logic in the caller.
	current atomic.Uint64 // Current ID available to hand out
	limit   atomic.Uint64 // Limit of the current block
}

// NewRangeAllocator creates a new allocator backed by BadgerDB.
func NewRangeAllocator(db *badger.DB, blockSize uint64) (*RangeAllocator, error) {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}

	alloc := &RangeAllocator{
		db:        db,
		blockSize: blockSize,
	}

	// Load the global counter from disk
	if err := alloc.loadGlobalCounter(); err != nil {
		return nil, err
	}

	// Initialize current/limit to trigger immediate allocation on first use
	// internal ids 1-indexed.
	// If globalMax is 0, we start at 1.
	// We set current = globalMax, limit = globalMax to force a refill.
	alloc.current.Store(alloc.globalMax)
	alloc.limit.Store(alloc.globalMax)

	return alloc, nil
}

// Allocate allocates a single ID.
// Thread-safe and high performance.
func (r *RangeAllocator) Allocate() (uint64, error) {
	for {
		// 1. Try to increment current ID
		val := r.current.Add(1)
		limit := r.limit.Load()

		// 2. If within limit, return it
		if val <= limit {
			return val, nil
		}

		// 3. If exceeded, we need to reserve a new block
		// We use a lock to ensure only one goroutine reserves a block at a time
		// Optimization: Check again after locking
		r.mu.Lock()

		// Re-read atomic values inside lock to see if another thread already updated them
		current := r.current.Load()
		limit = r.limit.Load()

		if current <= limit {
			// Another thread already allocated a new block
			r.mu.Unlock()
			continue // Retry the allocation loop
		}

		// Truly exhausted, reserve new block
		if err := r.reserveBlock(); err != nil {
			r.mu.Unlock()
			return 0, err
		}

		// Reset current to the start of the new block (globalMax - blockSize)
		// We want the next Add(1) to return (start + 1)
		// So we set current to start.
		newStart := r.globalMax - r.blockSize
		r.current.Store(newStart)
		r.limit.Store(r.globalMax)

		r.mu.Unlock()
	}
}

// AllocateBatch allocates a batch of n IDs.
// Returns the start ID of the batch (end ID = start + n - 1).
func (r *RangeAllocator) AllocateBatch(n uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// For batch allocation, we might need to extend the global counter directly
	// to avoid fragmenting the current block if n is large.
	// For simplicity and correctness, we always reserve directly from global
	// if the request is larger than remaining or just generally for batches.
	// ACTUALLY: Let's keep it simple. We update globalMax by n (or max(n, blockSize)).

	// 1. Update Global Counter
	oldGlobal := r.globalMax
	r.globalMax += n

	// 2. Persist
	if err := r.saveGlobalCounter(); err != nil {
		r.globalMax = oldGlobal // Rollback
		return 0, err
	}

	// 3. Return start of range (oldGlobal + 1)
	return oldGlobal + 1, nil
}

// reserveBlock updates the global counter to reserve a new block of IDs.
// Must be called with mu locked.
func (r *RangeAllocator) reserveBlock() error {
	r.globalMax += r.blockSize
	return r.saveGlobalCounter()
}

func (r *RangeAllocator) loadGlobalCounter() error {
	return r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(globalCounterKey))
		if err == badger.ErrKeyNotFound {
			r.globalMax = 0
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) >= 8 {
				r.globalMax = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})
}

func (r *RangeAllocator) saveGlobalCounter() error {
	return r.db.Update(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, r.globalMax)
		return txn.Set([]byte(globalCounterKey), buf)
	})
}

// CurrentID returns the highest ID allocated so far (approximate).
func (r *RangeAllocator) CurrentID() uint64 {
	return r.current.Load()
}
