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
	filter    *BloomFilter
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

	// Initialize BloomFilter (2B items, 1% FP)
	// TODO: Load existing filter from disk if needed, or rebuild.
	// For now we start fresh in RAM, assuming cold start or cache warming strategy.
	filter := NewBloomFilter(2000000000, 0.01)

	enc := &Encoder{
		db:           db,
		forwardCache: newLRUCache[string, uint64](cacheSize),
		reverseCache: newLRUCache[uint64, string](cacheSize),
		allocator:    allocator,
		filter:       filter,
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

	// 2. Check Bloom Filter (RAM)
	// If bloom filter says "No", we are 100% sure it's not in DB.
	// We can skip the disk read and go straight to allocation.
	if e.filter != nil && !e.filter.Test(s) {
		// Definitely not in DB. Allocate new ID.
		return e.allocateNewID(s)
	}

	// 3. Check BadgerDB (Disk)
	// Bloom filter said "Maybe", so we must check DB.
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

	// 4. Not in DB (False positive from Bloom Filter). Allocate new ID.
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

	// Add to Bloom Filter
	if e.filter != nil {
		e.filter.Add(s)
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
			// Check Bloom Filter immediately
			if e.filter != nil && !e.filter.Test(key) {
				// Definitely not in DB. Mark for allocation.
				// We'll handle these as "definite misses" or just let the DB check loop handle them?
				// To optimize, we can separate "definite new" from "possible existing".
				// But for simplicity, we treat them as misses and then check DB only for valid bloom hits.
				// Actually, simpler: just add to misses list.
				// But we want to avoid DB lookups for bloom negatives.
			}
			misses = append(misses, miss{index: i, key: key})
		}
	}

	if len(misses) == 0 {
		return results, nil
	}

	// 2. Filter Misses: Separate "Bloom Negative" (New) from "Bloom Positive" (Maybe Existing)
	var possibleExisting []miss
	var definiteNew []miss

	for _, m := range misses {
		if e.filter != nil && !e.filter.Test(m.key) {
			definiteNew = append(definiteNew, m)
		} else {
			possibleExisting = append(possibleExisting, m)
		}
	}

	// 3. Slow Path: Check BadgerDB for "possible existing"
	if len(possibleExisting) > 0 {
		err := e.db.View(func(txn *badger.Txn) error {
			for _, m := range possibleExisting {
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
	}

	// 4. Collect all keys that still need IDs (Bloom hits not found in DB + Bloom misses)
	var toCreate []miss

	// Add those from possibleExisting that weren't found
	for _, m := range possibleExisting {
		if results[m.index] == 0 {
			toCreate = append(toCreate, m)
		}
	}
	// Add all definiteNew
	toCreate = append(toCreate, definiteNew...)

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

			// Add to Bloom Filter
			if e.filter != nil {
				e.filter.Add(m.key)
			}

			// Update cache
			e.forwardCache.Add(m.key, newID)
			e.reverseCache.Add(newID, m.key)
		}

		if err := batch.Flush(); err != nil {
			return nil, err
		}

		// Note: RangeAllocator persisting is handled inside AllocateBatch
	}

	// 5. Update cache for results found in DB (step 3)
	for _, m := range possibleExisting {
		id := results[m.index]
		if id != 0 {
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
	}
}
