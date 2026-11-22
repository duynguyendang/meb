package vector

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

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
	idx.tombstones[packedID] = struct{}{}
	idx.tombstonesMu.Unlock()
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

func (idx *HNSWIndex) Compact(ctx context.Context, topicID uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	idx.graphMu.Lock()
	defer idx.graphMu.Unlock()

	prefix := keys.HNSWTopicPrefix(topicID)
	tombstoneTopicPrefix := []byte{keys.HNSWPrefix, 0xFF,
		byte(topicID >> 16), byte(topicID >> 8), byte(topicID)}

	type nodeEntry struct {
		packedID uint64
		level    int
	}
	type neighborUpdate struct {
		key       []byte
		neighbors []uint64
	}

	var toRemove []nodeEntry
	removedUnique := make(map[uint64]struct{})
	var tombstoneKeys [][]byte
	tombstoneKeySet := make(map[string]struct{})
	deletedSet := make(map[uint64]struct{})
	var neighborUpdates []neighborUpdate

	var bestSurvivor uint64
	bestSurvivorLevel := -1

	currentEntry, hasEP := idx.getEntryPoint(topicID)

	err := idx.db.View(func(txn *badger.Txn) error {
		// First scan persisted tombstones for this topic: [0x12][0xFF][topicID:3].
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
			tombstoneKeys = append(tombstoneKeys, tk)
			tombstoneKeySet[string(tk)] = struct{}{}
			deletedID := binary.BigEndian.Uint64(tk[2:10])
			deletedSet[deletedID] = struct{}{}
		}

		// Also include any in-memory tombstones for this topic.
		idx.tombstonesMu.RLock()
		for id := range idx.tombstones {
			if keys.UnpackTopicID(id) == topicID {
				deletedSet[id] = struct{}{}
			}
		}
		idx.tombstonesMu.RUnlock()

		if len(deletedSet) == 0 && len(tombstoneKeys) == 0 {
			return nil
		}

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			key := item.Key()
			if len(key) < 10 || key[0] != keys.HNSWPrefix {
				continue
			}
			if _, isTombstone := tombstoneKeySet[string(key)]; isTombstone {
				continue
			}

			packedID := binary.BigEndian.Uint64(key[1:9])
			level := int(key[9])

			if _, deleted := deletedSet[packedID]; deleted {
				toRemove = append(toRemove, nodeEntry{packedID: packedID, level: level})
				removedUnique[packedID] = struct{}{}
				continue
			}

			if level > bestSurvivorLevel {
				bestSurvivorLevel = level
				bestSurvivor = packedID
			}

			// Prune any deleted IDs from surviving node's neighbor list.
			var pruned []uint64
			changed := false
			if err := item.Value(func(val []byte) error {
				count := len(val) / 8
				for i := 0; i < count; i++ {
					nid := binary.BigEndian.Uint64(val[i*8:])
					if _, nidDeleted := deletedSet[nid]; nidDeleted {
						if !changed {
							changed = true
							pruned = make([]uint64, 0, count)
							for j := 0; j < i; j++ {
								pruned = append(pruned, binary.BigEndian.Uint64(val[j*8:]))
							}
						}
						continue
					}
					if changed {
						pruned = append(pruned, nid)
					}
				}
				return nil
			}); err != nil {
				return err
			}
			if changed {
				kCopy := make([]byte, len(key))
				copy(kCopy, key)
				neighborUpdates = append(neighborUpdates, neighborUpdate{
					key:       kCopy,
					neighbors: pruned,
				})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(toRemove) == 0 && len(tombstoneKeys) == 0 && len(neighborUpdates) == 0 {
		return nil
	}

	err = idx.db.Update(func(txn *badger.Txn) error {
		for _, entry := range toRemove {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := keys.EncodeHNSWKey(entry.packedID, entry.level)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		for _, upd := range neighborUpdates {
			if err := ctx.Err(); err != nil {
				return err
			}
			val := make([]byte, len(upd.neighbors)*8)
			for i, n := range upd.neighbors {
				binary.BigEndian.PutUint64(val[i*8:], n)
			}
			if err := txn.Set(upd.key, val); err != nil {
				return err
			}
		}
		for _, tk := range tombstoneKeys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := txn.Delete(tk); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if _, epDeleted := deletedSet[currentEntry]; hasEP && epDeleted {
		idx.entryPointMu.Lock()
		idx.maxLevelMu.Lock()
		if bestSurvivorLevel >= 0 {
			idx.entryPoints[topicID] = bestSurvivor
			idx.maxLevels[topicID] = bestSurvivorLevel
		} else {
			delete(idx.entryPoints, topicID)
			delete(idx.maxLevels, topicID)
		}
		idx.maxLevelMu.Unlock()
		idx.entryPointMu.Unlock()
	}

	if len(removedUnique) > 0 {
		idx.nodeCountMu.Lock()
		if idx.nodeCounts[topicID] >= len(removedUnique) {
			idx.nodeCounts[topicID] -= len(removedUnique)
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

	// Remove compacted nodes from in-memory tombstone cache.
	idx.tombstonesMu.Lock()
	for id := range deletedSet {
		delete(idx.tombstones, id)
	}
	idx.tombstonesMu.Unlock()

	return nil
}
