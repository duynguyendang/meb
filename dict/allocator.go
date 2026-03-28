package dict

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

const (
	DefaultBlockSize = 10000

	globalCounterKey = "__dict_global_counter"
)

type RangeAllocator struct {
	db        *badger.DB
	blockSize uint64

	globalMax uint64
	mu        sync.Mutex

	current atomic.Uint64
	limit   atomic.Uint64
}

func NewRangeAllocator(db *badger.DB, blockSize uint64) (*RangeAllocator, error) {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}

	alloc := &RangeAllocator{
		db:        db,
		blockSize: blockSize,
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

func (r *RangeAllocator) CurrentID() uint64 {
	return r.current.Load()
}
