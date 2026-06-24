package vector

import (
	"context"
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

func (idx *HNSWIndex) writeNode(ctx context.Context, packedID uint64, level int, neighbors []uint64) error {
	key := keys.EncodeHNSWKey(packedID, level)

	val := make([]byte, len(neighbors)*8)
	for i, n := range neighbors {
		binary.BigEndian.PutUint64(val[i*8:], n)
	}

	return idx.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (idx *HNSWIndex) readNeighbors(packedID uint64, level int) ([]uint64, error) {
	key := keys.EncodeHNSWKey(packedID, level)

	var neighbors []uint64
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
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

func (idx *HNSWIndex) deleteNode(packedID uint64, level int) error {
	key := keys.EncodeHNSWKey(packedID, level)
	return idx.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
