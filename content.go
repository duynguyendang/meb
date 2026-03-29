package meb

import (
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/klauspost/compress/s2"
)

func (m *MEBStore) SetContent(id uint64, data []byte) error {
	compressed := s2.Encode(nil, data)
	key := keys.EncodeChunkKey(id)
	return m.withWriteTxn(func(txn *badger.Txn) error {
		return txn.Set(key, compressed)
	})
}

func (m *MEBStore) GetContent(id uint64) ([]byte, error) {
	key := keys.EncodeChunkKey(id)

	var data []byte
	err := m.withReadTxn(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get content: %w", err)
	}

	decompressed, err := s2.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress content: %w", err)
	}

	if decompressed == nil && len(data) > 0 {
		return []byte{}, nil
	}

	return decompressed, nil
}

func (m *MEBStore) AddDocument(docKey string, content []byte, vec []float32, metadata map[string]any) error {
	if docKey == "" {
		return fmt.Errorf("%w: document key cannot be empty", ErrInvalidFact)
	}

	id, err := m.dict.GetOrCreateID(docKey)
	if err != nil {
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// Add metadata facts first — if this fails, nothing else is written
	if len(metadata) > 0 {
		facts := make([]Fact, 0, len(metadata))
		for key, value := range metadata {
			facts = append(facts, Fact{
				Subject:   docKey,
				Predicate: key,
				Object:    value,
			})
		}
		if err := m.AddFactBatch(facts); err != nil {
			return fmt.Errorf("failed to add metadata facts: %w", err)
		}
	}

	if len(vec) > 0 {
		if err := m.vectors.Add(id, vec); err != nil {
			return fmt.Errorf("failed to add vector: %w", err)
		}
	}

	if len(content) > 0 {
		if err := m.SetContent(id, content); err != nil {
			return fmt.Errorf("failed to store content: %w", err)
		}
	}

	return nil
}

func (m *MEBStore) GetDocumentMetadata(docKey string) (map[string]any, error) {
	if docKey == "" {
		return nil, fmt.Errorf("document key cannot be empty")
	}

	metadata := make(map[string]any)

	for fact, err := range m.Scan(docKey, "", "") {
		if err != nil {
			return nil, fmt.Errorf("failed to scan metadata: %w", err)
		}
		metadata[fact.Predicate] = fact.Object
	}

	return metadata, nil
}

func (m *MEBStore) DeleteDocument(docKey string) error {
	if docKey == "" {
		return fmt.Errorf("document key cannot be empty")
	}

	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	contentKey := keys.EncodeChunkKey(id)
	if err := m.withWriteTxn(func(txn *badger.Txn) error {
		return txn.Delete(contentKey)
	}); err != nil && err != badger.ErrKeyNotFound {
		return fmt.Errorf("failed to delete content: %w", err)
	}

	if !m.vectors.Delete(id) {
		slog.Debug("vector not found for deletion", "key", docKey, "id", id)
	}

	// Pack TopicID into subject ID — facts are stored with packed IDs
	packedID := keys.PackID(m.topicID, keys.UnpackLocalID(id))
	metadataPrefix := keys.EncodeTripleSPOPrefix(packedID, 0, 0)

	var deletedCount uint64
	if err := m.withWriteTxn(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(metadataPrefix); it.ValidForPrefix(metadataPrefix); it.Next() {
			item := it.Item()
			key := item.Key()
			s, p, o := keys.DecodeTripleKey(key)
			opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)
			if err := txn.Delete(key); err != nil {
				return err
			}
			if err := txn.Delete(opsKey); err != nil {
				return err
			}
			deletedCount++
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to delete metadata facts: %w", err)
	}

	if deletedCount > 0 {
		m.numFacts.Add(^uint64(deletedCount - 1))
	}

	return nil
}

func (m *MEBStore) HasDocument(docKey string) (bool, error) {
	if docKey == "" {
		return false, nil
	}

	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get document ID: %w", err)
	}

	contentKey := keys.EncodeChunkKey(id)
	var hasContent bool
	if err := m.withReadTxn(func(txn *badger.Txn) error {
		_, err := txn.Get(contentKey)
		if err == nil {
			hasContent = true
		}
		return nil
	}); err != nil && err != badger.ErrKeyNotFound {
		return false, fmt.Errorf("failed to check content: %w", err)
	}
	if hasContent {
		return true, nil
	}

	if m.vectors.HasVector(id) {
		return true, nil
	}

	metadataPrefix := keys.EncodeTripleSPOPrefix(keys.PackID(m.topicID, keys.UnpackLocalID(id)), 0, 0)
	var hasMetadata bool
	if err := m.withReadTxn(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(metadataPrefix)
		if it.ValidForPrefix(metadataPrefix) {
			hasMetadata = true
		}
		return nil
	}); err != nil {
		return false, fmt.Errorf("failed to check metadata: %w", err)
	}

	return hasMetadata, nil
}

func (m *MEBStore) GetContentByKey(docKey string) ([]byte, error) {
	id, err := m.dict.GetID(docKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get document ID: %w", err)
	}
	return m.GetContent(id)
}

func (m *MEBStore) LookupID(key string) (uint64, bool) {
	id, err := m.dict.GetID(key)
	return id, err == nil
}
