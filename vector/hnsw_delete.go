package vector

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type hnswCompactState struct {
	cursor            []byte
	tombstoneKeys     [][]byte
	removedUnique     map[uint64]struct{}
	bestSurvivor      uint64
	bestSurvivorLevel int
}

func (idx *HNSWIndex) getOrCreateTombstoneCounter(topicID uint32) *atomic.Int64 {
	idx.tombstoneCountsMu.RLock()
	c := idx.tombstoneCounts[topicID]
	idx.tombstoneCountsMu.RUnlock()
	if c != nil {
		return c
	}
	idx.tombstoneCountsMu.Lock()
	defer idx.tombstoneCountsMu.Unlock()
	if c = idx.tombstoneCounts[topicID]; c != nil {
		return c
	}
	c = &atomic.Int64{}
	idx.tombstoneCounts[topicID] = c
	return c
}

// TombstoneCount returns the number of uncompacted soft-deletes recorded for topicID.
func (idx *HNSWIndex) TombstoneCount(topicID uint32) int64 {
	idx.tombstoneCountsMu.RLock()
	c := idx.tombstoneCounts[topicID]
	idx.tombstoneCountsMu.RUnlock()
	if c == nil {
		return 0
	}
	return c.Load()
}

// TombstoneRatio returns tombstones / nodeCount for topicID (0 if empty).
func (idx *HNSWIndex) TombstoneRatio(topicID uint32) float64 {
	tombs := idx.TombstoneCount(topicID)
	if tombs <= 0 {
		return 0
	}
	idx.nodeCountMu.RLock()
	total := idx.nodeCounts[topicID]
	idx.nodeCountMu.RUnlock()
	if total <= 0 {
		return 1.0
	}
	return float64(tombs) / float64(total)
}

// ShouldCompact reports whether topicID's tombstone ratio meets or exceeds
// CompactionRatio (default 15%).
func (idx *HNSWIndex) ShouldCompact(topicID uint32) bool {
	threshold := idx.cfg.CompactionRatio
	if threshold <= 0 {
		threshold = DefaultHNSWCompactionRatio
	}
	return idx.TombstoneCount(topicID) > 0 && idx.TombstoneRatio(topicID) >= threshold
}

// ActiveTopics returns topicIDs tracked by the HNSW index.
func (idx *HNSWIndex) ActiveTopics() []uint32 {
	idx.nodeCountMu.RLock()
	topics := make([]uint32, 0, len(idx.nodeCounts))
	for tid := range idx.nodeCounts {
		topics = append(topics, tid)
	}
	idx.nodeCountMu.RUnlock()
	return topics
}

func (idx *HNSWIndex) SoftDelete(packedID uint64) error {
	tombstoneKey := keys.EncodeHNSWTombstoneKey(packedID)

	now := time.Now().UnixNano()
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(now))

	if err := idx.db.Update(func(txn *badger.Txn) error {
		return txn.Set(tombstoneKey, val)
	}); err != nil {
		return err
	}

	idx.tombstonesMu.Lock()
	_, already := idx.tombstones[packedID]
	idx.tombstones[packedID] = struct{}{}
	idx.tombstonesMu.Unlock()

	if !already {
		idx.getOrCreateTombstoneCounter(keys.UnpackTopicID(packedID)).Add(1)
	}
	return nil
}

func (idx *HNSWIndex) isDeleted(packedID uint64) bool {
	tombstoneKey := keys.EncodeHNSWTombstoneKey(packedID)

	err := idx.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(tombstoneKey)
		return err
	})
	return err == nil
}

// Compact runs incremental compaction passes until all tombstoned nodes and
// dangling neighbor references for topicID have been cleaned up.
func (idx *HNSWIndex) Compact(ctx context.Context, topicID uint32) error {
	batchSize := idx.cfg.CompactionBatchSize
	if batchSize <= 0 {
		batchSize = DefaultHNSWCompactionBatchSize
	}
	for {
		_, done, err := idx.CompactIncremental(ctx, topicID, batchSize)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// CompactIncremental processes up to batchSize HNSW node-level keys for topicID
// using fine-grained striped node locks instead of a global index mutex.
// Returns (processed, done, err), where done=true marks completion of the topic pass.
func (idx *HNSWIndex) CompactIncremental(ctx context.Context, topicID uint32, batchSize int) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if batchSize <= 0 {
		batchSize = idx.cfg.CompactionBatchSize
		if batchSize <= 0 {
			batchSize = DefaultHNSWCompactionBatchSize
		}
	}

	idx.compactMu.Lock()
	defer idx.compactMu.Unlock()

	prefix := keys.HNSWTopicPrefix(topicID)
	tombstoneTopicPrefix := []byte{keys.HNSWPrefix, 0xFF,
		byte(topicID >> 16), byte(topicID >> 8), byte(topicID)}

	state := idx.compactStates[topicID]
	if state == nil {
		state = &hnswCompactState{
			removedUnique:     make(map[uint64]struct{}),
			bestSurvivorLevel: -1,
		}
		idx.compactStates[topicID] = state
	}

	type nodeEntry struct {
		packedID uint64
		level    int
	}
	type neighborUpdate struct {
		packedID uint64
		level    int
		key      []byte
	}

	deletedSet := make(map[uint64]struct{})
	tombstoneKeySet := make(map[string]struct{})

	// 1. Collect persisted tombstones for topicID (first pass or fresh lookup).
	err := idx.db.View(func(txn *badger.Txn) error {
		tit := txn.NewIterator(badger.DefaultIteratorOptions)
		defer tit.Close()
		for tit.Seek(tombstoneTopicPrefix); tit.ValidForPrefix(tombstoneTopicPrefix); tit.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			rawKey := tit.Item().Key()
			if len(rawKey) < 10 {
				continue
			}
			tk := make([]byte, len(rawKey))
			copy(tk, rawKey)
			tombstoneKeySet[string(tk)] = struct{}{}
			deletedID := binary.BigEndian.Uint64(tk[2:10])
			deletedSet[deletedID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	// Merge persisted tombstone keys discovered at start of pass.
	if state.cursor == nil && len(tombstoneKeySet) > 0 {
		for k := range tombstoneKeySet {
			state.tombstoneKeys = append(state.tombstoneKeys, []byte(k))
		}
	}

	// Also include in-memory tombstones for this topic.
	idx.tombstonesMu.RLock()
	for id := range idx.tombstones {
		if keys.UnpackTopicID(id) == topicID {
			deletedSet[id] = struct{}{}
		}
	}
	idx.tombstonesMu.RUnlock()

	if len(deletedSet) == 0 && len(state.tombstoneKeys) == 0 && state.cursor == nil {
		delete(idx.compactStates, topicID)
		return 0, true, nil
	}

	// 2. Scan up to batchSize HNSW node-level keys starting at cursor (or prefix).
	var toRemove []nodeEntry
	var toPrune []neighborUpdate
	processed := 0
	reachedEnd := true
	var nextCursor []byte

	err = idx.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		seekPos := prefix
		if state.cursor != nil {
			seekPos = state.cursor
		}

		for it.Seek(seekPos); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Item().Key()
			if len(key) < 10 || key[0] != keys.HNSWPrefix {
				continue
			}
			if _, isTombstone := tombstoneKeySet[string(key)]; isTombstone {
				continue
			}

			if processed >= batchSize {
				nextCursor = make([]byte, len(key))
				copy(nextCursor, key)
				reachedEnd = false
				break
			}
			processed++

			packedID := binary.BigEndian.Uint64(key[1:9])
			level := int(key[9])

			if _, deleted := deletedSet[packedID]; deleted {
				toRemove = append(toRemove, nodeEntry{packedID: packedID, level: level})
				state.removedUnique[packedID] = struct{}{}
				continue
			}

			if level > state.bestSurvivorLevel {
				state.bestSurvivorLevel = level
				state.bestSurvivor = packedID
			}

			kCopy := make([]byte, len(key))
			copy(kCopy, key)
			toPrune = append(toPrune, neighborUpdate{
				packedID: packedID,
				level:    level,
				key:      kCopy,
			})
		}
		return nil
	})
	if err != nil {
		return processed, false, err
	}

	// 3. Apply per-node deletions & neighbor pruning using fine-grained node stripe locks.
	for _, entry := range toRemove {
		if err := ctx.Err(); err != nil {
			return processed, false, err
		}
		if err := idx.deleteNode(entry.packedID, entry.level); err != nil {
			return processed, false, err
		}
	}

	for _, upd := range toPrune {
		if err := ctx.Err(); err != nil {
			return processed, false, err
		}
		lock := idx.nodeStripe(upd.packedID)
		lock.Lock()
		neighbors, err := idx.readNeighborsUnlocked(upd.packedID, upd.level)
		if err == nil && len(neighbors) > 0 {
			changed := false
			pruned := make([]uint64, 0, len(neighbors))
			for _, nid := range neighbors {
				if _, nidDeleted := deletedSet[nid]; nidDeleted {
					changed = true
					continue
				}
				pruned = append(pruned, nid)
			}
			if changed {
				err = idx.writeNodeUnlocked(ctx, upd.packedID, upd.level, pruned)
			}
		}
		lock.Unlock()
		if err != nil && err != badger.ErrKeyNotFound {
			return processed, false, err
		}
	}

	if !reachedEnd {
		state.cursor = nextCursor
		return processed, false, nil
	}

	// 4. Finalize pass: delete tombstone keys, rotate entry point if deleted, clear caches/counters.
	if len(state.tombstoneKeys) > 0 {
		if err := idx.db.Update(func(txn *badger.Txn) error {
			for _, tk := range state.tombstoneKeys {
				if err := txn.Delete(tk); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return processed, false, err
		}
	}

	currentEntry, hasEP := idx.getEntryPoint(topicID)
	if _, epDeleted := deletedSet[currentEntry]; hasEP && epDeleted {
		idx.entryPointMu.Lock()
		idx.maxLevelMu.Lock()
		if state.bestSurvivorLevel >= 0 {
			idx.entryPoints[topicID] = state.bestSurvivor
			idx.maxLevels[topicID] = state.bestSurvivorLevel
		} else {
			delete(idx.entryPoints, topicID)
			delete(idx.maxLevels, topicID)
		}
		idx.maxLevelMu.Unlock()
		idx.entryPointMu.Unlock()
	}

	if len(state.removedUnique) > 0 {
		idx.nodeCountMu.Lock()
		if idx.nodeCounts[topicID] >= len(state.removedUnique) {
			idx.nodeCounts[topicID] -= len(state.removedUnique)
		} else {
			idx.nodeCounts[topicID] = 0
		}
		idx.nodeCountMu.Unlock()
	}

	idx.vecCacheMu.Lock()
	for id := range deletedSet {
		delete(idx.vecCache, id)
	}
	idx.vecCacheMu.Unlock()

	idx.tombstonesMu.Lock()
	for id := range deletedSet {
		delete(idx.tombstones, id)
	}
	idx.tombstonesMu.Unlock()

	idx.getOrCreateTombstoneCounter(topicID).Store(0)
	delete(idx.compactStates, topicID)
	return processed, true, nil
}
