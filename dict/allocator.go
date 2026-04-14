package dict

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

const (
	DefaultBlockSize = 10000

	globalCounterKey = "__dict_global_counter"
)

type ShardedAllocator struct {
	shards []*RangeAllocator
	n      uint32
	next   atomic.Uint32
}

func NewShardedAllocator(db *badger.DB, blockSize uint64, numShards int) (*ShardedAllocator, error) {
	if numShards <= 1 {
		alloc, err := NewRangeAllocator(db, blockSize)
		if err != nil {
			return nil, err
		}
		return &ShardedAllocator{
			shards: []*RangeAllocator{alloc},
			n:      1,
		}, nil
	}

	shards := make([]*RangeAllocator, numShards)
	for i := 0; i < numShards; i++ {
		shard, err := NewRangeAllocatorWithKey(db, blockSize, fmt.Sprintf("%s_%d", globalCounterKey, i))
		if err != nil {
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, fmt.Errorf("failed to create shard %d: %w", i, err)
		}
		shards[i] = shard
	}

	return &ShardedAllocator{
		shards: shards,
		n:      uint32(numShards),
	}, nil
}

func (s *ShardedAllocator) Allocate() (uint64, error) {
	shard := s.nextShard()
	return shard.Allocate()
}

func (s *ShardedAllocator) AllocateBatch(n uint64) (uint64, error) {
	shard := s.nextShard()
	return shard.AllocateBatch(n)
}

func (s *ShardedAllocator) CurrentID() uint64 {
	var maxID uint64
	for _, shard := range s.shards {
		id := shard.CurrentID()
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}

func (s *ShardedAllocator) NumShards() int {
	return int(s.n)
}

// Reset clears all shards and resets the global counter.
func (s *ShardedAllocator) Reset() error {
	for _, shard := range s.shards {
		if err := shard.Reset(); err != nil {
			return fmt.Errorf("failed to reset shard: %w", err)
		}
	}
	s.next.Store(0)
	return nil
}

func (s *ShardedAllocator) nextShard() *RangeAllocator {
	if s.n == 1 {
		return s.shards[0]
	}
	idx := s.next.Add(1) % s.n
	return s.shards[idx]
}

func (s *ShardedAllocator) Close() {
	for _, shard := range s.shards {
		shard.Close()
	}
}

type RangeAllocator struct {
	db         *badger.DB
	counterKey string
	blockSize  uint64

	globalMax uint64
	mu        sync.Mutex

	current atomic.Uint64
	limit   atomic.Uint64
}

func NewRangeAllocator(db *badger.DB, blockSize uint64) (*RangeAllocator, error) {
	return NewRangeAllocatorWithKey(db, blockSize, globalCounterKey)
}

func NewRangeAllocatorWithKey(db *badger.DB, blockSize uint64, counterKey string) (*RangeAllocator, error) {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}

	alloc := &RangeAllocator{
		db:         db,
		counterKey: counterKey,
		blockSize:  blockSize,
	}

	if err := alloc.loadGlobalCounter(); err != nil {
		return nil, err
	}

	alloc.current.Store(alloc.globalMax)
	alloc.limit.Store(alloc.globalMax)

	return alloc, nil
}

func (r *RangeAllocator) Allocate() (uint64, error) {
	for {
		val := r.current.Add(1)
		limit := r.limit.Load()

		if val <= limit {
			return val, nil
		}

		r.mu.Lock()

		current := r.current.Load()
		limit = r.limit.Load()

		if current <= limit {
			r.mu.Unlock()
			continue
		}

		if err := r.reserveBlock(); err != nil {
			r.mu.Unlock()
			return 0, err
		}

		newStart := r.globalMax - r.blockSize
		r.current.Store(newStart)
		r.limit.Store(r.globalMax)

		r.mu.Unlock()
	}
}

func (r *RangeAllocator) AllocateBatch(n uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldGlobal := r.globalMax
	r.globalMax += n

	if err := r.saveGlobalCounter(); err != nil {
		r.globalMax = oldGlobal
		return 0, err
	}

	return oldGlobal + 1, nil
}

func (r *RangeAllocator) reserveBlock() error {
	r.globalMax += r.blockSize
	return r.saveGlobalCounter()
}

func (r *RangeAllocator) loadGlobalCounter() error {
	return r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(r.counterKey))
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
		return txn.Set([]byte(r.counterKey), buf)
	})
}

func (r *RangeAllocator) CurrentID() uint64 {
	return r.current.Load()
}

// Reset clears the allocator state and resets the global counter to 0.
func (r *RangeAllocator) Reset() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.globalMax = 0
	r.current.Store(0)
	r.limit.Store(0)

	return r.saveGlobalCounter()
}

func (r *RangeAllocator) Close() {}
