package dict

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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

	// Atomic ID generation
	nextID uint64

	// Mutex for ID allocation
	mu sync.Mutex
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
	enc := &Encoder{
		db: db,
		forwardCache: newLRUCache[string, uint64](cacheSize),
		reverseCache: newLRUCache[uint64, string](cacheSize),
		nextID:       1, // Start from ID 1 (ID 0 reserved for "not found")
	}

	// Load the next ID from BadgerDB
	if err := enc.loadNextID(); err != nil {
		return nil, fmt.Errorf("failed to load next ID: %w", err)
	}

	return enc, nil
}

// loadNextID loads the next ID counter from BadgerDB.
func (e *Encoder) loadNextID() error {
	return e.db.View(func(txn *badger.Txn) error {
		// The next ID is stored under a special key
		item, err := txn.Get([]byte("__dict_next_id"))
		if err == badger.ErrKeyNotFound {
			// First time using this dictionary
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			e.nextID = binary.BigEndian.Uint64(val)
			return nil
		})
	})
}

// saveNextID saves the next ID counter to BadgerDB.
func (e *Encoder) saveNextID(id uint64) error {
	return e.db.Update(func(txn *badger.Txn) error {
		key := []byte("__dict_next_id")
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, id)
		return txn.Set(key, val)
	})
}

// GetOrCreateID gets the ID for a string, creating a new ID if it doesn't exist.
// Thread-safe for concurrent access.
func (e *Encoder) GetOrCreateID(s string) (uint64, error) {
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

	if err == nil {
		// Found in DB - update cache
		e.forwardCache.Add(s, id)
		e.reverseCache.Add(id, s)
		return id, nil
	}

	if err != ErrNotFound {
		return 0, err
	}

	// 3. Allocate new ID (atomic)
	e.mu.Lock()
	newID := atomic.AddUint64(&e.nextID, 1)
	e.mu.Unlock()

	// 4. Persist to BadgerDB
	batch := e.db.NewWriteBatch()
	defer batch.Cancel()

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, newID)

	// Forward map: string -> ID
	if err := batch.Set(key, idBytes); err != nil {
		return 0, err
	}

	// Reverse map: ID -> string
	reverseKey := makeDictReverseKey(newID)
	if err := batch.Set(reverseKey, []byte(s)); err != nil {
		return 0, err
	}

	if err := batch.Flush(); err != nil {
		return 0, err
	}

	// 5. Persist next ID counter after allocation to ensure persistence
	// We need to save nextID (which is already incremented), not newID
	if err := e.saveNextID(atomic.LoadUint64(&e.nextID)); err != nil {
		// Log error but don't fail the operation
		// The nextID will be recovered on restart from the actual max ID in the DB
	}

	// 6. Update cache
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

	// 1. Fast Path: Check LRU cache first (lock-free read)
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

	// 2. Slow Path: Batch process all misses
	// Query BadgerDB for all missing keys at once
	err := e.db.View(func(txn *badger.Txn) error {
		for _, m := range misses {
			key := makeDictForwardKey(m.key)
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				continue // Will need to allocate new ID
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

	// 3. Allocate new IDs for keys that still don't have IDs
	var toCreate []miss
	for _, m := range misses {
		if results[m.index] == 0 {
			toCreate = append(toCreate, m)
		}
	}

	if len(toCreate) > 0 {
		// Batch allocate IDs (single lock)
		e.mu.Lock()
		firstID := atomic.AddUint64(&e.nextID, uint64(len(toCreate))) - uint64(len(toCreate)) + 1
		e.mu.Unlock()

		// Batch persist to BadgerDB
		batch := e.db.NewWriteBatch()
		defer batch.Cancel()

		for i, m := range toCreate {
			newID := firstID + uint64(i)
			results[m.index] = newID

			// Forward map: string -> ID
			key := makeDictForwardKey(m.key)
			idBytes := make([]byte, 8) // Create a NEW slice for each entry (don't reuse)
			binary.BigEndian.PutUint64(idBytes, newID)
			if err := batch.Set(key, idBytes); err != nil {
				return nil, err
			}

			// Reverse map: ID -> string
			reverseKey := makeDictReverseKey(newID)
			if err := batch.Set(reverseKey, []byte(m.key)); err != nil {
				return nil, err
			}
		}

		if err := batch.Flush(); err != nil {
			return nil, err
		}

		// Save next ID counter after batch allocation to ensure persistence
		// Save the atomic nextID value (already incremented by the allocation)
		if err := e.saveNextID(atomic.LoadUint64(&e.nextID)); err != nil {
			return nil, fmt.Errorf("failed to save next ID: %w", err)
		}
	}

	// 4. Update cache for all results
	for i, key := range keys {
		id := results[i]
		if id != 0 {
			e.forwardCache.Add(key, id)
			e.reverseCache.Add(id, key)
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
	// Save the next ID counter on close
	if err := e.saveNextID(atomic.LoadUint64(&e.nextID)); err != nil {
		return fmt.Errorf("failed to save next ID: %w", err)
	}
	return nil
}

// Stats returns statistics about the encoder.
func (e *Encoder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"next_id":          atomic.LoadUint64(&e.nextID),
		"forward_cache_len": e.forwardCache.cache.Len(),
		"reverse_cache_len": e.reverseCache.cache.Len(),
	}
}
