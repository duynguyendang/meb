package dict

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// dictNextIDKey is the key for storing the next ID counter
const dictNextIDKey = "__dict_next_id"

// ShardedEncoder implements a sharded bi-directional String <-> Uint64 mapping.
// It uses multiple shards to reduce lock contention in concurrent workloads.
// Each shard has its own mutex and LRU cache.
type ShardedEncoder struct {
	db *badger.DB

	// Shards for concurrent access
	shards []*encoderShard
	numShards int

	// Next ID allocation (atomic)
	nextID uint64

	// Mutex for ID allocation (only used during allocation)
	allocMu sync.Mutex
}

// encoderShard represents a single shard of the dictionary
type encoderShard struct {
	mu sync.RWMutex

	// In-memory LRU cache
	forwardCache *lruCache[string, uint64]
	reverseCache *lruCache[uint64, string]
}

// NewShardedEncoder creates a new sharded dictionary encoder.
// numShards should be a power of 2 for optimal distribution.
func NewShardedEncoder(db *badger.DB, cacheSize int, numShards int) (*ShardedEncoder, error) {
	if numShards < 1 {
		numShards = 1
	}
	if numShards > 256 {
		numShards = 256
	}

	// Ensure numShards is a power of 2
	if numShards&(numShards-1) != 0 {
		// Round up to next power of 2
		power := 1
		for power < numShards {
			power <<= 1
		}
		numShards = power
	}

	shards := make([]*encoderShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &encoderShard{
			forwardCache: newLRUCache[string, uint64](cacheSize / numShards),
			reverseCache: newLRUCache[uint64, string](cacheSize / numShards),
		}
	}

	enc := &ShardedEncoder{
		db:         db,
		shards:     shards,
		numShards:  numShards,
		nextID:     1, // Start from ID 1 (ID 0 reserved)
	}

	// Load the next ID from BadgerDB
	if err := enc.loadNextID(); err != nil {
		return nil, fmt.Errorf("failed to load next ID: %w", err)
	}

	return enc, nil
}

// getShard returns the shard index for a given string key
func (e *ShardedEncoder) getShard(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) & (e.numShards - 1)
}

// GetOrCreateID gets the ID for a string, creating a new ID if it doesn't exist.
// Thread-safe for concurrent access with minimal contention.
func (e *ShardedEncoder) GetOrCreateID(s string) (uint64, error) {
	shardIdx := e.getShard(s)
	shard := e.shards[shardIdx]

	// 1. Check shard's LRU cache
	if id, ok := shard.forwardCache.Get(s); ok && id != 0 {
		return id, nil
	}

	// 2. Check shard's LRU cache with read lock
	shard.mu.RLock()
	if id, ok := shard.forwardCache.Get(s); ok && id != 0 {
		shard.mu.RUnlock()
		return id, nil
	}
	shard.mu.RUnlock()

	// 3. Check BadgerDB
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
		// Found in DB - update cache with write lock
		shard.mu.Lock()
		shard.forwardCache.Add(s, id)
		shard.reverseCache.Add(id, s)
		shard.mu.Unlock()
		return id, nil
	}

	if err != ErrNotFound {
		return 0, err
	}

	// 4. Allocate new ID
	e.allocMu.Lock()
	newID := atomic.AddUint64(&e.nextID, 1)
	e.allocMu.Unlock()

	// 5. Persist to BadgerDB
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

	// 6. Update cache with write lock
	shard.mu.Lock()
	shard.forwardCache.Add(s, newID)
	shard.reverseCache.Add(newID, s)
	shard.mu.Unlock()

	// 7. Persist next ID counter periodically
	if newID%1000 == 0 {
		_ = e.saveNextID(newID) // Ignore errors, will be recovered on restart
	}

	return newID, nil
}

// GetIDs gets IDs for multiple strings in a batch, creating new IDs as needed.
// This is more efficient than calling GetOrCreateID multiple times.
// Keys are grouped by shard to minimize lock contention.
func (e *ShardedEncoder) GetIDs(keys []string) ([]uint64, error) {
	results := make([]uint64, len(keys))

	// Track misses grouped by shard: shardID -> []{index, key}
	type miss struct {
		index int
		key   string
	}
	missesByShard := make(map[int][]miss)

	// 1. Fast Path: Check LRU cache first (lock-free read)
	for i, key := range keys {
		shardIdx := e.getShard(key)
		shard := e.shards[shardIdx]

		if id, ok := shard.forwardCache.Get(key); ok && id != 0 {
			results[i] = id
		} else {
			// Group misses by shard
			missesByShard[shardIdx] = append(missesByShard[shardIdx], miss{index: i, key: key})
		}
	}

	if len(missesByShard) == 0 {
		return results, nil
	}

	// 2. Slow Path: Batch process all misses
	// Query BadgerDB for all missing keys at once
	err := e.db.View(func(txn *badger.Txn) error {
		for _, misses := range missesByShard {
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
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 3. Allocate new IDs for keys that still don't have IDs (grouped by shard)
	var toCreateByShard = make(map[int][]miss)
	for shardIdx, misses := range missesByShard {
		for _, m := range misses {
			if results[m.index] == 0 {
				toCreateByShard[shardIdx] = append(toCreateByShard[shardIdx], m)
			}
		}
	}

	if len(toCreateByShard) > 0 {
		// Count total number of new IDs needed
		totalToCreate := 0
		for _, toCreate := range toCreateByShard {
			totalToCreate += len(toCreate)
		}

		// Batch allocate IDs (single global lock)
		e.allocMu.Lock()
		firstID := atomic.AddUint64(&e.nextID, uint64(totalToCreate)) - uint64(totalToCreate) + 1
		e.allocMu.Unlock()

		// Batch persist to BadgerDB
		batch := e.db.NewWriteBatch()
		defer batch.Cancel()

		idBytes := make([]byte, 8)
		currentID := firstID

		for _, toCreate := range toCreateByShard {
			for _, m := range toCreate {
				newID := currentID
				currentID++
				results[m.index] = newID

				// Forward map: string -> ID
				key := makeDictForwardKey(m.key)
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
		}

		if err := batch.Flush(); err != nil {
			return nil, err
		}

		// Save next ID counter periodically
		lastID := firstID + uint64(totalToCreate-1)
		if lastID%1000 == 0 {
			_ = e.saveNextID(lastID)
		}
	}

	// 4. Update cache for all results (grouped by shard)
	for i, key := range keys {
		id := results[i]
		if id != 0 {
			shardIdx := e.getShard(key)
			shard := e.shards[shardIdx]
			shard.mu.Lock()
			shard.forwardCache.Add(key, id)
			shard.reverseCache.Add(id, key)
			shard.mu.Unlock()
		}
	}

	return results, nil
}

// GetString gets the string for an ID.
// Returns ErrNotFound if the ID doesn't exist.
func (e *ShardedEncoder) GetString(id uint64) (string, error) {
	// Hash the ID to find the shard
	// We use the ID directly since we need consistent reverse lookup
	shardIdx := int(id) & (e.numShards - 1)
	shard := e.shards[shardIdx]

	// 1. Check shard's LRU cache
	if s, ok := shard.reverseCache.Get(id); ok && s != "" {
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
	shard.mu.Lock()
	shard.reverseCache.Add(id, s)
	shard.mu.Unlock()

	return s, nil
}

// GetID gets the ID for a string without creating a new one.
// Returns ErrNotFound if the string doesn't exist.
func (e *ShardedEncoder) GetID(s string) (uint64, error) {
	shardIdx := e.getShard(s)
	shard := e.shards[shardIdx]

	// 1. Check shard's LRU cache
	if id, ok := shard.forwardCache.Get(s); ok && id != 0 {
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
	shard.mu.Lock()
	shard.forwardCache.Add(s, id)
	shard.mu.Unlock()

	return id, nil
}

// loadNextID loads the next ID counter from BadgerDB.
func (e *ShardedEncoder) loadNextID() error {
	return e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(dictNextIDKey))
		if err == badger.ErrKeyNotFound {
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
func (e *ShardedEncoder) saveNextID(id uint64) error {
	return e.db.Update(func(txn *badger.Txn) error {
		key := []byte(dictNextIDKey)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, id)
		return txn.Set(key, val)
	})
}

// Close flushes any pending state and releases resources.
func (e *ShardedEncoder) Close() error {
	// Save the next ID counter on close
	if err := e.saveNextID(atomic.LoadUint64(&e.nextID)); err != nil {
		return fmt.Errorf("failed to save next ID: %w", err)
	}
	return nil
}

// Stats returns statistics about the encoder.
func (e *ShardedEncoder) Stats() map[string]interface{} {
	totalForwardSize := 0
	totalReverseSize := 0

	for _, shard := range e.shards {
		totalForwardSize += shard.forwardCache.cache.Len()
		totalReverseSize += shard.reverseCache.cache.Len()
	}

	return map[string]interface{}{
		"num_shards":        e.numShards,
		"next_id":           atomic.LoadUint64(&e.nextID),
		"total_cache_entries": totalForwardSize + totalReverseSize,
		"forward_cache_len":  totalForwardSize,
		"reverse_cache_len":  totalReverseSize,
	}
}
