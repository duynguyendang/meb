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
