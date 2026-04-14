package meb

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/dict"
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
	return m.AddDocumentWithTopic(m.topicID.Load(), docKey, content, vec, metadata)
}

func (m *MEBStore) AddDocumentWithTopic(topicID uint32, docKey string, content []byte, vec []float32, metadata map[string]any) error {
	if topicID == 0 {
		return fmt.Errorf("%w: topicID must be non-zero", ErrInvalidFact)
	}
	return m.addDocumentWithTopic(topicID, docKey, content, vec, metadata)
}

func (m *MEBStore) addDocumentWithTopic(topicID uint32, docKey string, content []byte, vec []float32, metadata map[string]any) error {
	if docKey == "" {
		return fmt.Errorf("%w: document key cannot be empty", ErrInvalidFact)
	}

	// All operations in a single atomic transaction
	return m.Update(func(txn *StoreTxn) error {
		// Get or create dictionary ID
		id, err := txn.GetOrCreateID(docKey)
		if err != nil {
			return fmt.Errorf("failed to get document ID: %w", err)
		}

		// Add metadata facts
		if len(metadata) > 0 {
			facts := make([]Fact, 0, len(metadata))
			for key, value := range metadata {
				facts = append(facts, Fact{
					Subject:   docKey,
					Predicate: key,
					Object:    value,
				})
			}
			if err := txn.AddFactBatchWithTopic(facts, topicID); err != nil {
				return fmt.Errorf("failed to add metadata facts: %w", err)
			}
		}

		// Add vector
		if len(vec) > 0 {
			if err := txn.AddVector(id, vec); err != nil {
				return fmt.Errorf("failed to add vector: %w", err)
			}
			// Also update in-memory registry for fast access
			m.vectors.AddWithHash(id, vec, 0)
		}

		// Store content
		if len(content) > 0 {
			if err := txn.SetContent(id, content); err != nil {
				return fmt.Errorf("failed to store content: %w", err)
			}
		}

		return nil
	})
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
	if m.config.ReadOnly {
		return ErrStoreReadOnly
	}
	if docKey == "" {
		return fmt.Errorf("document key cannot be empty")
	}

	return m.DeleteDocumentWithTopic(docKey, m.topicID.Load())
}

func (m *MEBStore) DeleteDocumentWithTopic(docKey string, topicID uint32) error {
	if m.config.ReadOnly {
		return ErrStoreReadOnly
	}
	if docKey == "" {
		return fmt.Errorf("document key cannot be empty")
	}

	return m.Update(func(txn *StoreTxn) error {
		return txn.DeleteDocumentWithTopic(docKey, topicID)
	})
}

func (m *MEBStore) HasDocument(docKey string) (bool, error) {
	if docKey == "" {
		return false, nil
	}

	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound || err == dict.ErrNotFound {
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

	metadataPrefix := keys.EncodeTripleSPOPrefix(keys.PackID(m.topicID.Load(), keys.UnpackLocalID(id)), 0, 0)
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
