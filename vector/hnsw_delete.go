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

	return idx.db.Update(func(txn *badger.Txn) error {
		return txn.Set(tombstoneKey, val)
	})
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
	prefix := keys.HNSWTopicPrefix(topicID)
	tombstoneTopicPrefix := []byte{keys.HNSWPrefix, 0xFF,
		byte(topicID >> 16), byte(topicID >> 8), byte(topicID)}

	type nodeEntry struct {
		packedID uint64
		level    int
	}
	var toRemove []nodeEntry
	var tombstoneKeys [][]byte
	var currentEntry uint64

	currentEntry = idx.getEntryPoint(topicID)

	err := idx.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < 10 || key[0] != keys.HNSWPrefix {
				continue
			}
			if key[1] == 0xFF {
				// Tombstone marker found via topic prefix (only for topicID >= 0xFF0000)
				tk := make([]byte, len(key))
				copy(tk, key)
				tombstoneKeys = append(tombstoneKeys, tk)
				continue
			}
			packedID := binary.BigEndian.Uint64(key[1:9])

			if idx.isDeleted(packedID) {
				toRemove = append(toRemove, nodeEntry{packedID, int(key[9])})
				continue
			}
		}

		// Scan tombstone keys that the topic prefix missed.
		// Tombstone format: [0x12][0xFF][packedNodeID:8]
		// The topic prefix scan only matches tombstones when topicID>>16 == 0xFF
		// because position 1 is 0xFF, not a topicID byte. For all other topicIDs
		// we need a dedicated scan using [0x12][0xFF][topicID_bytes:3].
		tit := txn.NewIterator(badger.DefaultIteratorOptions)
		defer tit.Close()
		for tit.Seek(tombstoneTopicPrefix); tit.ValidForPrefix(tombstoneTopicPrefix); tit.Next() {
			tk := make([]byte, len(tit.Item().Key()))
			copy(tk, tit.Item().Key())
			tombstoneKeys = append(tombstoneKeys, tk)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(toRemove) == 0 && len(tombstoneKeys) == 0 {
		return nil
	}

	return idx.db.Update(func(txn *badger.Txn) error {
		for _, entry := range toRemove {
			key := keys.EncodeHNSWKey(entry.packedID, entry.level)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		for _, tk := range tombstoneKeys {
			if err := txn.Delete(tk); err != nil {
				return err
			}
		}
		if currentEntry != 0 && idx.isDeleted(currentEntry) {
			// Current entry point is deleted — pick a new one from any remaining
			// active node in this topic.
			newEntry := uint64(0)
			dit := txn.NewIterator(badger.DefaultIteratorOptions)
			defer dit.Close()
			for dit.Seek(prefix); dit.ValidForPrefix(prefix); dit.Next() {
				key := dit.Item().Key()
				if len(key) < 10 || key[0] != keys.HNSWPrefix || key[1] == 0xFF {
					continue
				}
				packedID := binary.BigEndian.Uint64(key[1:9])
				if newEntry == 0 {
					newEntry = packedID
				}
				_ = key[9] // level byte
			}
			idx.entryPointMu.Lock()
			if newEntry != 0 {
				idx.entryPoints[topicID] = newEntry
			} else {
				delete(idx.entryPoints, topicID)
			}
			idx.entryPointMu.Unlock()
		}
		return nil
	})
}
