package vector

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

func (idx *HNSWIndex) readLevel(packedID uint64) (int, error) {
	prefix := keys.HNSWNodePrefix(packedID)
	maxLevel := 0

	err := idx.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) >= 10 && key[0] == keys.HNSWPrefix {
				lvl := int(key[9])
				if lvl > maxLevel {
					maxLevel = lvl
				}
			}
		}
		return nil
	})

	return maxLevel, err
}

func (idx *HNSWIndex) readNeighborsAtLevel(packedID uint64, level int) ([]uint64, error) {
	key := keys.EncodeHNSWKey(packedID, level)

	var neighbors []uint64
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			count := len(val) / 8
			neighbors = make([]uint64, count)
			for i := 0; i < count; i++ {
				neighbors[i] = binary.BigEndian.Uint64(val[i*8:])
			}
			return nil
		})
	})
	return neighbors, err
}
