package dict

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

var (
	ErrNotFound = errors.New("key not found in dictionary")
)

const (
	dictForwardPrefix = byte(0x80)
	dictReversePrefix = byte(0x81)
)

type Encoder struct {
	db *badger.DB

	forwardCache *lruCache[string, uint64]
	reverseCache *lruCache[uint64, string]

	allocator *RangeAllocator
}

type lruCache[K comparable, V any] struct {
	cache *expirable.LRU[K, V]
}

func newLRUCache[K comparable, V any](size int) *lruCache[K, V] {
	return &lruCache[K, V]{
		cache: expirable.NewLRU[K, V](size, nil, 0),
	}
}

func (c *lruCache[K, V]) Get(key K) (V, bool) {
	return c.cache.Get(key)
}

func (c *lruCache[K, V]) Add(key K, value V) {
	c.cache.Add(key, value)
}

func NewEncoder(db *badger.DB, cacheSize int) (*Encoder, error) {
	allocator, err := NewRangeAllocator(db, DefaultBlockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create range allocator: %w", err)
	}

	enc := &Encoder{
		db:           db,
		forwardCache: newLRUCache[string, uint64](cacheSize),
		reverseCache: newLRUCache[uint64, string](cacheSize),
		allocator:    allocator,
	}

	return enc, nil
}

func (e *Encoder) GetOrCreateID(s string) (uint64, error) {
	if id, ok := e.forwardCache.Get(s); ok && id != 0 {
		return id, nil
	}

	key := makeDictForwardKey(s)
	var id uint64
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			id = binary.BigEndian.Uint64(val)
			return nil
		})
	})

	if err == nil {
		e.forwardCache.Add(s, id)
		e.reverseCache.Add(id, s)
		return id, nil
	}

	if err != ErrNotFound {
		return 0, err
	}

	return e.allocateNewID(s)
}

func (e *Encoder) allocateNewID(s string) (uint64, error) {
	newID, err := e.allocator.Allocate()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate ID: %w", err)
	}

	err = e.db.Update(func(txn *badger.Txn) error {
		key := makeDictForwardKey(s)
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, newID)
		if err := txn.Set(key, idBytes); err != nil {
			return err
		}

		reverseKey := makeDictReverseKey(newID)
		if err := txn.Set(reverseKey, []byte(s)); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	e.forwardCache.Add(s, newID)
	e.reverseCache.Add(newID, s)

	return newID, nil
}

func (e *Encoder) GetIDs(keys []string) ([]uint64, error) {
	results := make([]uint64, len(keys))

	type miss struct {
		index int
		key   string
	}
	var misses []miss

	batchLocalMap := make(map[string]uint64)

	for i, key := range keys {
		if id, ok := e.forwardCache.Get(key); ok && id != 0 {
			results[i] = id
		} else if id, ok := batchLocalMap[key]; ok {
			results[i] = id
		} else {
			misses = append(misses, miss{index: i, key: key})
			batchLocalMap[key] = 0
		}
	}

	if len(misses) == 0 {
		return results, nil
	}

	var toCreate []miss

	err := e.db.View(func(txn *badger.Txn) error {
		for _, m := range misses {
			key := makeDictForwardKey(m.key)
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				toCreate = append(toCreate, m)
				continue
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				id := binary.BigEndian.Uint64(val)
				results[m.index] = id
				batchLocalMap[m.key] = id
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(toCreate) > 0 {
		slog.Debug("dictionary allocating new IDs",
			"count", len(toCreate),
		)

		startID, err := e.allocator.AllocateBatch(uint64(len(toCreate)))
		if err != nil {
			return nil, fmt.Errorf("failed to allocate batch: %w", err)
		}

		batch := e.db.NewWriteBatch()
		defer batch.Cancel()

		for i, m := range toCreate {
			newID := startID + uint64(i)
			results[m.index] = newID
			batchLocalMap[m.key] = newID

			key := makeDictForwardKey(m.key)
			idBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(idBytes, newID)
			if err := batch.Set(key, idBytes); err != nil {
				return nil, err
			}

			reverseKey := makeDictReverseKey(newID)
			if err := batch.Set(reverseKey, []byte(m.key)); err != nil {
				return nil, err
			}

			e.forwardCache.Add(m.key, newID)
			e.reverseCache.Add(newID, m.key)
		}

		if err := batch.Flush(); err != nil {
			return nil, err
		}
	}

	for _, m := range misses {
		id := results[m.index]
		if id != 0 {
			e.forwardCache.Add(m.key, id)
			e.reverseCache.Add(id, m.key)
		}
	}

	for i, key := range keys {
		if results[i] == 0 {
			results[i] = batchLocalMap[key]
		}
	}

	return results, nil
}

func (e *Encoder) GetString(id uint64) (string, error) {
	if s, ok := e.reverseCache.Get(id); ok && s != "" {
		return s, nil
	}

	key := makeDictReverseKey(id)
	var s string
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			s = string(val)
			return nil
		})
	})

	if err != nil {
		return "", err
	}

	e.reverseCache.Add(id, s)

	return s, nil
}

func (e *Encoder) GetID(s string) (uint64, error) {
	if id, ok := e.forwardCache.Get(s); ok && id != 0 {
		return id, nil
	}

	key := makeDictForwardKey(s)
	var id uint64
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			id = binary.BigEndian.Uint64(val)
			return nil
		})
	})

	if err != nil {
		return 0, err
	}

	e.forwardCache.Add(s, id)

	return id, nil
}

func makeDictForwardKey(s string) []byte {
	if len(s) > 255 {
		hash := sha256.Sum256([]byte(s))
		key := make([]byte, 3+len(hash))
		key[0] = dictForwardPrefix
		binary.BigEndian.PutUint16(key[1:3], 0xFFFF)
		copy(key[3:], hash[:])
		return key
	}

	key := make([]byte, 3+len(s))
	key[0] = dictForwardPrefix
	binary.BigEndian.PutUint16(key[1:3], uint16(len(s)))
	copy(key[3:], s)
	return key
}

func makeDictReverseKey(id uint64) []byte {
	key := make([]byte, 9)
	key[0] = dictReversePrefix
	binary.BigEndian.PutUint64(key[1:9], id)
	return key
}

func (e *Encoder) Close() error {
	stats := e.Stats()
	slog.Info("dictionary closed",
		"forwardCacheLen", stats["forward_cache_len"],
		"reverseCacheLen", stats["reverse_cache_len"],
	)

	return nil
}

func (e *Encoder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"forward_cache_len": e.forwardCache.cache.Len(),
		"reverse_cache_len": e.reverseCache.cache.Len(),
		"next_id":           e.allocator.CurrentID() + 1,
	}
}
