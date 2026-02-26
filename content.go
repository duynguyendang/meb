package meb

import (
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/klauspost/compress/s2"
)

// SetContent stores compressed content for a given ID.
// The content is compressed using S2 compression before storage.
func (m *MEBStore) SetContent(id uint64, data []byte) error {
	// Compress the data using S2
	compressed := s2.Encode(nil, data)

	// Create the key
	key := keys.EncodeChunkKey(id)

	// Store in BadgerDB using transaction wrapper
	return m.withWriteTxn(func(txn *badger.Txn) error {
		return txn.Set(key, compressed)
	})
}

// GetContent retrieves and decompresses content for a given ID.
// Returns the original decompressed bytes.
// If the content is not found, returns nil with no error.
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
		// Key not found is not an error - content is optional
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get content: %w", err)
	}

	// Decompress the data
	decompressed, err := s2.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress content: %w", err)
	}

	// Handle edge case: if decompressed is nil but data was not empty, return empty slice
	if decompressed == nil && len(data) > 0 {
		return []byte{}, nil
	}

	return decompressed, nil
}

// AddDocument adds a complete document with vector, content, and metadata.
// This is a high-level helper that handles the full RAG pipeline.
func (m *MEBStore) AddDocument(docKey string, content []byte, vec []float32, metadata map[string]any) error {
	if docKey == "" {
		return fmt.Errorf("%w: document key cannot be empty", ErrInvalidFact)
	}

	slog.Debug("adding document",
		"key", docKey,
		"contentSize", len(content),
		"vectorDim", len(vec),
		"metadataCount", len(metadata),
	)

	// 1. Get or create ID for the document
	id, err := m.dict.GetOrCreateID(docKey)
	if err != nil {
		slog.Error("failed to get document ID", "key", docKey, "error", err)
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// 2. Store vector
	if len(vec) > 0 {
		if err := m.vectors.Add(id, vec); err != nil {
			slog.Error("failed to add vector", "key", docKey, "error", err)
			return fmt.Errorf("failed to add vector: %w", err)
		}
	}

	// 3. Store content (compressed)
	if len(content) > 0 {
		if err := m.SetContent(id, content); err != nil {
			slog.Error("failed to store content", "key", docKey, "error", err)
			return fmt.Errorf("failed to store content: %w", err)
		}
	}

	// 4. Store metadata as facts (as quads: subject, predicate, object, graph)
	if len(metadata) > 0 {
		for key, value := range metadata {
			fact := Fact{
				Subject:   docKey,
				Predicate: key,
				Object:    value,
				Graph:     "metadata",
			}
			if err := m.AddFact(fact); err != nil {
				slog.Error("failed to add metadata fact", "key", docKey, "predicate", key, "error", err)
				return fmt.Errorf("failed to add metadata fact for %s: %w", key, err)
			}
		}
	}

	slog.Debug("document added successfully", "key", docKey, "id", id)
	return nil
}

// GetDocumentMetadata retrieves all metadata for a document.
// Returns metadata as a map of key->value pairs from facts in the "metadata" graph.
func (m *MEBStore) GetDocumentMetadata(docKey string) (map[string]any, error) {
	if docKey == "" {
		return nil, fmt.Errorf("document key cannot be empty")
	}

	metadata := make(map[string]any)

	// Scan for facts in "metadata" graph where subject matches docKey
	for fact, err := range m.Scan(docKey, "", "", "metadata") {
		if err != nil {
			return nil, fmt.Errorf("failed to scan metadata: %w", err)
		}
		metadata[fact.Predicate] = fact.Object
	}

	return metadata, nil
}

// DeleteDocument deletes all data associated with a document:
// - Content (if any)
// - Vector (if any)
// - Metadata facts (in "metadata" graph)
func (m *MEBStore) DeleteDocument(docKey string) error {
	if docKey == "" {
		return fmt.Errorf("document key cannot be empty")
	}

	slog.Debug("deleting document", "key", docKey)

	// 1. Get the document ID
	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil // Already doesn't exist
		}
		return fmt.Errorf("failed to get document ID: %w", err)
	}

	// 2. Delete content
	contentKey := keys.EncodeChunkKey(id)
	if err := m.withWriteTxn(func(txn *badger.Txn) error {
		return txn.Delete(contentKey)
	}); err != nil && err != badger.ErrKeyNotFound {
		slog.Error("failed to delete content", "key", docKey, "error", err)
		return fmt.Errorf("failed to delete content: %w", err)
	}

	// 3. Delete vector (from registry)
	if !m.vectors.Delete(id) {
		slog.Debug("vector not found for deletion", "key", docKey, "id", id)
	}

	// 4. Delete metadata facts (scan and delete)
	// We need to collect keys first, then delete
	metadataGraphID, _ := m.dict.GetID("metadata")
	metadataKeyPrefix := keys.EncodeQuadSPOGPrefix(id, 0, 0, metadataGraphID)

	if err := m.withWriteTxn(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(metadataKeyPrefix); it.ValidForPrefix(metadataKeyPrefix); it.Next() {
			item := it.Item()
			if err := txn.Delete(item.Key()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		slog.Error("failed to delete metadata facts", "key", docKey, "error", err)
		return fmt.Errorf("failed to delete metadata facts: %w", err)
	}

	slog.Debug("document deleted successfully", "key", docKey)
	return nil
}

// HasDocument checks if a document exists (has content, vector, or metadata).
func (m *MEBStore) HasDocument(docKey string) (bool, error) {
	if docKey == "" {
		return false, nil
	}

	// Check if document ID exists in dictionary
	id, err := m.dict.GetID(docKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get document ID: %w", err)
	}

	// Check for content
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

	// Check for vector
	if m.vectors.HasVector(id) {
		return true, nil
	}

	// Check for metadata facts
	metadataGraphID, err := m.dict.GetID("metadata")
	if err == nil {
		metadataKeyPrefix := keys.EncodeQuadSPOGPrefix(id, 0, 0, metadataGraphID)
		var hasMetadata bool
		if err := m.withReadTxn(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			it.Seek(metadataKeyPrefix)
			if it.ValidForPrefix(metadataKeyPrefix) {
				hasMetadata = true
			}
			return nil
		}); err != nil {
			return false, fmt.Errorf("failed to check metadata: %w", err)
		}
		if hasMetadata {
			return true, nil
		}
	}

	return false, nil
}
