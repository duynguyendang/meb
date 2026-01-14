// Package meb implements a high-performance, memory-efficient bidirectional graph store
// using BadgerDB and dictionary encoding. It supports quad-based facts (Subject-Predicate-Object-Graph)
// with dual indexing (SPO and OPS) for efficient bidirectional traversal.
//
// Features:
//   - Dictionary encoding for memory efficiency
//   - Dual indices (SPO/OPS) for bidirectional graph traversal
//   - Atomic operations with transaction pooling
//   - Batched operations for high throughput
//   - Vector search integration
//   - Multi-tenancy via graph contexts
//
// Example usage:
//
//	cfg := &store.Config{DataDir: "./data", DictDir: "./dict"}
//	s, err := meb.NewMEBStore(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Close()
//
//	// Add facts
//	fact := meb.NewFact("Alice", "knows", "Bob")
//	s.AddFact(fact)
//
//	// Query facts
//	for f, err := range s.Scan("Alice", "", "", "") {
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    fmt.Printf("%s\n", f.String())
//	}
package meb

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/predicates"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/mangle/ast"
)

// MEBStore implements both factstore.FactStore and store.KnowledgeStore interfaces.
// It uses BadgerDB for persistent storage and dictionary encoding for efficient operations.
type MEBStore struct {
	db     *badger.DB
	dictDB *badger.DB // Separate DB for dictionary
	dict   dict.Dictionary

	// Predicate tables
	predicates map[ast.PredicateSym]*predicates.PredicateTable

	// Configuration
	config *store.Config

	// Mutex for predicate table registration
	mu sync.RWMutex

	// numFacts tracks the total number of facts in RAM.
	// We use atomic.Uint64 for lock-free thread safety.
	// This value is persisted to disk only on graceful shutdown.
	numFacts atomic.Uint64

	// Vector registry for MRL vector search
	vectors *vector.VectorRegistry
}

// loadStats reads the counter from disk into RAM.
func (m *MEBStore) loadStats() error {
	return m.withReadTxn(func(txn *badger.Txn) error {
		item, err := txn.Get(keys.KeyFactCount)
		if err == badger.ErrKeyNotFound {
			m.numFacts.Store(0)
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) >= 8 {
				count := binary.BigEndian.Uint64(val)
				m.numFacts.Store(count)
			}
			return nil
		})
	})
}

// saveStats writes the RAM counter to disk.
func (m *MEBStore) saveStats() error {
	if m.config.ReadOnly {
		return nil
	}
	return m.withWriteTxn(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, m.numFacts.Load())
		return txn.Set(keys.KeyFactCount, buf)
	})
}

// NewMEBStore creates a new MEBStore with the given configuration.
func NewMEBStore(cfg *store.Config) (*MEBStore, error) {
	slog.Info("initializing MEB store",
		"dataDir", cfg.DataDir,
		"inMemory", cfg.InMemory,
		"blockCacheSize", cfg.BlockCacheSize,
		"indexCacheSize", cfg.IndexCacheSize,
		"numDictShards", cfg.NumDictShards,
	)

	// Validate configuration before proceeding
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Open BadgerDB for Facts
	db, err := store.OpenBadgerDB(cfg)
	if err != nil {
		slog.Error("failed to open BadgerDB", "error", err)
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	slog.Info("BadgerDB (Facts) opened successfully")

	// Open BadgerDB for Dictionary
	// Create a modified config for Dictionary DB
	dictCfg := *cfg // Copy config
	dictCfg.DataDir = cfg.DictDir
	dictCfg.SyncWrites = true // Enforce strict persistence for Dictionary

	dictDB, err := store.OpenBadgerDB(&dictCfg)
	if err != nil {
		db.Close()
		slog.Error("failed to open Dictionary BadgerDB", "error", err)
		return nil, fmt.Errorf("failed to open Dictionary BadgerDB: %w", err)
	}
	slog.Info("BadgerDB (Dictionary) opened successfully")

	// Create dictionary encoder (sharded if configured)
	var dictEncoder dict.Dictionary
	if cfg.NumDictShards > 0 {
		slog.Info("creating sharded dictionary encoder", "shards", cfg.NumDictShards, "lruCacheSize", cfg.LRUCacheSize)
		dictEncoder, err = dict.NewShardedEncoder(dictDB, cfg.LRUCacheSize, cfg.NumDictShards)
		if err != nil {
			dictDB.Close()
			db.Close()
			return nil, fmt.Errorf("failed to create sharded dictionary encoder: %w", err)
		}
	} else {
		slog.Info("creating single-threaded dictionary encoder", "lruCacheSize", cfg.LRUCacheSize)
		dictEncoder, err = dict.NewEncoder(dictDB, cfg.LRUCacheSize)
		if err != nil {
			dictDB.Close()
			db.Close()
			return nil, fmt.Errorf("failed to create dictionary encoder: %w", err)
		}
	}

	m := &MEBStore{
		db:         db,
		dictDB:     dictDB,
		dict:       dictEncoder,
		predicates: make(map[ast.PredicateSym]*predicates.PredicateTable),
		config:     cfg,
		vectors:    vector.NewRegistry(db),
	}

	// Load vector snapshot from disk
	if err := m.vectors.LoadSnapshot(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load vector snapshot: %w", err)
	}

	// Load fact count stats from disk
	if err := m.loadStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load stats: %w", err)
	}

	// Register default predicates (triples)
	m.registerDefaultPredicates()

	slog.Info("MEB store initialized successfully", "factCount", m.numFacts.Load())
	return m, nil
}

// registerDefaultPredicates registers the built-in predicates.
func (m *MEBStore) registerDefaultPredicates() {
	// Register "triples" predicate for subject-predicate-object relationships
	triplesPred := ast.PredicateSym{Symbol: "triples", Arity: 3}
	m.predicates[triplesPred] = predicates.NewPredicateTable(m.db, m.dict, triplesPred, keys.SPOPrefix)
}

// newTxn creates a new read-only transaction.
func (m *MEBStore) newTxn() *badger.Txn {
	return m.db.NewTransaction(false)
}

// releaseTxn discards the transaction.
func (m *MEBStore) releaseTxn(txn *badger.Txn) {
	txn.Discard()
}

// Reset clears the store by deleting all data.
func (m *MEBStore) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Info("resetting store", "factCount", m.numFacts.Load())

	// Clear all data
	err := m.db.DropAll()
	if err != nil {
		slog.Error("failed to drop all data", "error", err)
		return fmt.Errorf("failed to reset store: %w", err)
	}

	// Reset fact count (atomic operation)
	m.numFacts.Store(0)

	slog.Info("store reset complete")
	return nil
}

// Close closes the store and releases resources.
func (m *MEBStore) Close() error {
	slog.Info("closing store", "factCount", m.numFacts.Load())

	// Save vector snapshot before closing
	if !m.config.ReadOnly {
		if err := m.vectors.SaveSnapshot(); err != nil {
			slog.Error("failed to save vector snapshot", "error", err)
			return fmt.Errorf("failed to save vector snapshot: %w", err)
		}
	}

	// Save fact count stats to disk
	if err := m.saveStats(); err != nil {
		slog.Error("failed to save stats", "error", err)
		return fmt.Errorf("failed to save stats: %w", err)
	}

	// Wait for vector operations to complete
	if err := m.vectors.Close(); err != nil {
		slog.Error("failed to close vectors", "error", err)
		return err
	}

	// Close dictionary
	if err := m.dict.Close(); err != nil {
		slog.Error("failed to close dictionary", "error", err)
		return err
	}

	// Close Dictionary BadgerDB
	if err := m.dictDB.Close(); err != nil {
		slog.Error("failed to close dictionary database", "error", err)
		// We still try to close the main DB even if this fails
	}

	// Close BadgerDB
	if err := m.db.Close(); err != nil {
		slog.Error("failed to close database", "error", err)
		return err
	}

	slog.Info("store closed successfully")
	return nil
}

// Count returns the total number of facts in the store.
// This is a zero-cost atomic read from memory.
func (m *MEBStore) Count() uint64 {
	return m.numFacts.Load()
}

// RecalculateStats forces a full DB scan to fix the fact counter.
// This is an expensive operation that should only be used if the counter
// is suspected to be out of sync (e.g., after an unclean shutdown).
// It scans the SPOG index and updates both the in-memory counter and disk.
func (m *MEBStore) RecalculateStats() (uint64, error) {
	slog.Info("recalculating stats (expensive operation)", "currentCount", m.numFacts.Load())

	var count uint64

	err := m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Key-only is much faster
		it := txn.NewIterator(opts)
		defer it.Close()

		// Count only primary SPO keys
		// Note: We use SPOPrefix (0x01) because AddFactBatch currently writes 25-byte Triple keys.
		// Although QuadSPOGPrefix (0x20) is defined, it is not yet used for writing facts.
		prefix := []byte{keys.SPOPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			// Ensure we are counting valid keys
			if len(item.Key()) == keys.TripleKeySize {
				count++
			}
		}
		return nil
	})

	if err != nil {
		slog.Error("failed to recalculate stats", "error", err)
		return 0, fmt.Errorf("failed to recalculate stats: %w", err)
	}

	// Update RAM counter
	m.numFacts.Store(count)

	// Save to disk
	if err := m.saveStats(); err != nil {
		slog.Error("failed to save recalculated stats", "error", err)
		return 0, fmt.Errorf("failed to save recalculated stats: %w", err)
	}

	slog.Info("stats recalculated successfully", "newCount", count)
	return count, nil
}

// Vectors returns the vector registry for vector search operations.
func (m *MEBStore) Vectors() *vector.VectorRegistry {
	return m.vectors
}

// Find returns a new query builder for neuro-symbolic search.
// Example:
//
//	results, err := store.Find().
//	    SimilarTo(embedding).
//	    Where("author", "alice").
//	    Limit(5).
//	    Execute()
func (m *MEBStore) Find() *Builder {
	return NewBuilder(m)
}
