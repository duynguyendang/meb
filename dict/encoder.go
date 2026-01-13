package dict

import (
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

// Key prefixes for dictionary storage in BadgerDB
const (
	dictForwardPrefix = byte(0x80) // String -> ID
	dictReversePrefix = byte(0x81) // ID -> String
)

// Encoder implements bi-directional String <-> Uint64 mapping with LRU cache.
// It uses BadgerDB for persistent storage and an in-memory LRU cache for fast lookups.
type Encoder struct {
	db *badger.DB

	// In-memory LRU cache (expirable LRU with no expiration)
	forwardCache *lruCache[string, uint64] // string -> uint64
	reverseCache *lruCache[uint64, string] // uint64 -> string

	// Components
	allocator *RangeAllocator
}

// lruCache wraps expirable.LRU for a simpler API
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

// NewEncoder creates a new dictionary encoder with the given BadgerDB instance and cache size.
// If the dictionary already exists in BadgerDB, it loads the next ID counter.
func NewEncoder(db *badger.DB, cacheSize int) (*Encoder, error) {
	// Initialize Allocator
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

// Methods loadNextID and saveNextID removed as RangeAllocator handles this.

// GetOrCreateID gets the ID for a string, creating a new ID if it doesn't exist.
// Thread-safe for concurrent access.
func (e *Encoder) GetOrCreateID(s string) (uint64, error) {
	// 1. Check LRU cache (RAM)
	if id, ok := e.forwardCache.Get(s); ok && id != 0 {
		return id, nil
	}

	// 2. Check BadgerDB (Disk)
	// Badger's native Bloom Filter (if loaded) will optimize this check.
	// If native bloom says "No", txn.Get returns ErrKeyNotFound very quickly (no disk IO).
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
		// Found in DB - update cache
		e.forwardCache.Add(s, id)
		e.reverseCache.Add(id, s)
		return id, nil
	}

	if err != ErrNotFound {
		return 0, err
	}

	// 3. Not in DB. Allocate new ID.
	return e.allocateNewID(s)
}

// allocateNewID allocates a new ID for the given string using the RangeAllocator.
func (e *Encoder) allocateNewID(s string) (uint64, error) {
	// Allocate ID from RangeAllocator
	newID, err := e.allocator.Allocate()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate ID: %w", err)
	}

	// Persist to BadgerDB
	// Note: RangeAllocator already persisted the GlobalCounter range.
	// We just need to persist the mapping itself.

	// Use batch for better performance if possible, but here we are in single-item flow.
	// Ideally we should batch these updates, but the interface is single-item.
	// The persistent dictionary requirement implies we must write to disk or have a WAL.
	// BadgerDB handles this with WAL.

	err = e.db.Update(func(txn *badger.Txn) error {
		// Forward map: string -> ID
		key := makeDictForwardKey(s)
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, newID)
		if err := txn.Set(key, idBytes); err != nil {
			return err
		}

		// Reverse map: ID -> string
		reverseKey := makeDictReverseKey(newID)
		if err := txn.Set(reverseKey, []byte(s)); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	// Update cache
	e.forwardCache.Add(s, newID)
	e.reverseCache.Add(newID, s)

	return newID, nil
}

// GetIDs gets IDs for multiple strings in a batch, creating new IDs as needed.
// This is more efficient than calling GetOrCreateID multiple times.
func (e *Encoder) GetIDs(keys []string) ([]uint64, error) {
	results := make([]uint64, len(keys))

	// Track misses: index -> key
	type miss struct {
		index int
		key   string
	}
	var misses []miss

	// 1. Fast Path: Check LRU cache first (lock-free)
	for i, key := range keys {
		if id, ok := e.forwardCache.Get(key); ok && id != 0 {
			results[i] = id
		} else {
			misses = append(misses, miss{index: i, key: key})
		}
	}

	if len(misses) == 0 {
		return results, nil
	}

	// 2. Check BadgerDB for misses
	// We treat all cache misses as potential DB hits.
	// Badger's native bloom filter handles the negative lookups efficiently.
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
				results[m.index] = binary.BigEndian.Uint64(val)
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

		// Batch allocate IDs
		startID, err := e.allocator.AllocateBatch(uint64(len(toCreate)))
		if err != nil {
			return nil, fmt.Errorf("failed to allocate batch: %w", err)
		}

		// Batch persist to BadgerDB
		batch := e.db.NewWriteBatch()
		defer batch.Cancel()

		for i, m := range toCreate {
			newID := startID + uint64(i)
			results[m.index] = newID

			// Forward map: string -> ID
			key := makeDictForwardKey(m.key)
			idBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(idBytes, newID)
			if err := batch.Set(key, idBytes); err != nil {
				return nil, err
			}

			// Reverse map: ID -> string
			reverseKey := makeDictReverseKey(newID)
			if err := batch.Set(reverseKey, []byte(m.key)); err != nil {
				return nil, err
			}

			// Update cache
			e.forwardCache.Add(m.key, newID)
			e.reverseCache.Add(newID, m.key)
		}

		if err := batch.Flush(); err != nil {
			return nil, err
		}
	}

	// 3. Update cache for results found in DB
	for _, m := range misses {
		// If it was found (not added to toCreate, or added and now has result), update cache
		// Actually simpler: just iterate all original misses and check if they have result now
		// but we already updated cache for 'toCreate' items.
		// We just need to update cache for items found in DB (not in toCreate)

		id := results[m.index]
		if id != 0 {
			// Redundant add for toCreate items but safe/cheap
			e.forwardCache.Add(m.key, id)
			e.reverseCache.Add(id, m.key)
		}
	}

	return results, nil
}

// GetString gets the string for an ID.
// Returns ErrNotFound if the ID doesn't exist.
func (e *Encoder) GetString(id uint64) (string, error) {
	// 1. Check LRU cache
	if s, ok := e.reverseCache.Get(id); ok && s != "" {
		return s, nil
	}

	// 2. Check BadgerDB
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

	// 3. Update cache
	e.reverseCache.Add(id, s)

	return s, nil
}

// GetID gets the ID for a string without creating a new one.
// Returns ErrNotFound if the string doesn't exist.
func (e *Encoder) GetID(s string) (uint64, error) {
	// 1. Check LRU cache
	if id, ok := e.forwardCache.Get(s); ok && id != 0 {
		return id, nil
	}

	// 2. Check BadgerDB
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

	// 3. Update cache
	e.forwardCache.Add(s, id)

	return id, nil
}

// makeDictForwardKey creates a BadgerDB key for string -> ID lookup.
func makeDictForwardKey(s string) []byte {
	// Format: [0x80 | string_length(2) | string_bytes]
	key := make([]byte, 3+len(s))
	key[0] = dictForwardPrefix
	binary.BigEndian.PutUint16(key[1:3], uint16(len(s)))
	copy(key[3:], s)
	return key
}

// makeDictReverseKey creates a BadgerDB key for ID -> string lookup.
func makeDictReverseKey(id uint64) []byte {
	// Format: [0x81 | id(8)]
	key := make([]byte, 9)
	key[0] = dictReversePrefix
	binary.BigEndian.PutUint64(key[1:9], id)
	return key
}

// Close flushes any pending state and releases resources.
func (e *Encoder) Close() error {
	stats := e.Stats()
	slog.Info("dictionary closed",
		"forwardCacheLen", stats["forward_cache_len"],
		"reverseCacheLen", stats["reverse_cache_len"],
	)

	return nil
}

// Stats returns statistics about the encoder.
func (e *Encoder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"forward_cache_len": e.forwardCache.cache.Len(),
		"reverse_cache_len": e.reverseCache.cache.Len(),
		"next_id":           e.allocator.CurrentID() + 1,
	}
}
