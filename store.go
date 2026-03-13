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
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duynguyendang/meb/circuit"
	"github.com/duynguyendang/meb/clustering"
	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/predicates"
	"github.com/duynguyendang/meb/query"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"

	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/factstore"
	"github.com/dgraph-io/badger/v4"
)

// MEBStore implements both factstore.FactStore and store.KnowledgeStore interfaces.
// It uses BadgerDB for persistent storage and dictionary encoding for efficient operations.
type MEBStore struct {
	db     *badger.DB
	dictDB *badger.DB // Separate DB for dictionary
	dict   dict.Dictionary

	// Transaction pool for high-throughput scenarios
	txPool *TxPool

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

	// Circuit breaker for query timeout protection
	breaker *circuit.Breaker

	// graphsCache caches the list of graph names to avoid rescanning on every query
	graphsCache      []string
	graphsCacheValid bool

	// GC tracking for auto-compaction
	lastGCTime   time.Time
	factsSinceGC uint64

	// Temporal store for time-based facts (DatalogMTL support)
	temporalStore *MEBTemporalStore
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
		vectors:    vector.NewRegistry(db, nil),
		breaker:    circuit.NewBreaker(nil),
		txPool:     NewTxPool(db, 16),
	}

	m.txPool.Init()

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
	// Uses quad SPOG prefix (0x20) for 33-byte keys
	triplesPred := ast.PredicateSym{Symbol: "triples", Arity: 3}
	m.predicates[triplesPred] = predicates.NewPredicateTable(m.db, m.dict, triplesPred, keys.QuadSPOGPrefix)
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

	// Run GC before closing to compact the database
	if !m.config.ReadOnly && m.config.EnableAutoGC {
		gcRatio := m.config.GCRatio
		if gcRatio <= 0 {
			gcRatio = 0.5
		}
		slog.Info("running GC before shutdown", "ratio", gcRatio)

		// Run GC on facts DB
		if err := m.db.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
			slog.Warn("GC failed for facts DB", "error", err)
		}

		// Run GC on dictionary DB
		if err := m.dictDB.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
			slog.Warn("GC failed for dictionary DB", "error", err)
		}
	}

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

	// Close transaction pool
	m.txPool.Close()

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

		// Count only primary SPOG quad keys (33 bytes)
		prefix := []byte{keys.QuadSPOGPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			// Ensure we are counting valid quad keys
			if len(item.Key()) == keys.QuadKeySize {
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

// RunValueLogGC runs garbage collection on BadgerDB value logs to reclaim disk space.
// The ratio parameter specifies the ratio of garbage to data that must be exceeded
// for a value log file to be rewritten. Valid range is (0, 1). Lower values will
// reclaim more space but take longer.
func (m *MEBStore) RunValueLogGC(ratio float64) error {
	if ratio <= 0 || ratio > 1 {
		ratio = 0.5
	}

	slog.Info("running value log garbage collection", "ratio", ratio)

	// Run GC on facts DB
	if err := m.db.RunValueLogGC(ratio); err != nil && err != badger.ErrNoRewrite {
		slog.Error("failed to run GC on facts DB", "error", err)
		return fmt.Errorf("failed to run GC on facts DB: %w", err)
	} else if err == badger.ErrNoRewrite {
		slog.Info("no GC needed for facts DB")
	} else {
		slog.Info("GC completed on facts DB")
	}

	// Run GC on dictionary DB
	if err := m.dictDB.RunValueLogGC(ratio); err != nil && err != badger.ErrNoRewrite {
		slog.Error("failed to run GC on dictionary DB", "error", err)
		return fmt.Errorf("failed to run GC on dictionary DB: %w", err)
	} else if err == badger.ErrNoRewrite {
		slog.Info("no GC needed for dictionary DB")
	} else {
		slog.Info("GC completed on dictionary DB")
	}

	return nil
}

// CircuitBreaker returns the circuit breaker for query timeout protection.
func (m *MEBStore) CircuitBreaker() *circuit.Breaker {
	return m.breaker
}

// SetCircuitBreakerConfig updates the circuit breaker configuration.
// This allows runtime adjustment of query timeout and failure thresholds.
func (m *MEBStore) SetCircuitBreakerConfig(config *circuit.Config) {
	m.breaker = circuit.NewBreaker(config)
	slog.Info("circuit breaker configuration updated",
		"timeout", config.QueryTimeout,
		"failureThreshold", config.FailureThreshold,
	)
}

// CircuitBreakerMetrics returns the current circuit breaker metrics.
func (m *MEBStore) CircuitBreakerMetrics() circuit.Metrics {
	return m.breaker.Metrics()
}

// LFTJEngine returns a new Leapfrog Triejoin query engine.
// LFTJ provides worst-case optimal multi-way join performance (10-1000x faster than nested loops).
//
// Example usage:
//
//	engine := store.LFTJEngine()
//	query := query.LFTJQuery{...}
//	for result, err := range engine.Execute(ctx, query) {
//	    // Process result
//	}
func (m *MEBStore) LFTJEngine() *query.LFTJEngine {
	return query.NewLFTJEngine(m.db)
}

// ExecuteLFTJQuery executes a multi-way join query using Leapfrog Triejoin.
func (m *MEBStore) ResolveID(id uint64) (string, error) {
	return m.dict.GetString(id)
}

// This is the recommended method for complex multi-atom queries.
// Returns an iterator over joined tuples.
//
// Performance: O(N × |output|) vs O(|R₁| × |R₂| × ... × |Rₙ|) for nested loops
func (m *MEBStore) ExecuteLFTJQuery(ctx context.Context, q query.LFTJQuery) iter.Seq2[map[string]uint64, error] {
	engine := m.LFTJEngine()
	return engine.Execute(ctx, q)
}

// QueryDatalog executes a high-level Datalog query string and returns string results using the LFTJ Engine.
func (m *MEBStore) QueryDatalog(ctx context.Context, datalogQuery string) ([]map[string]string, error) {
	executor := query.NewExecutor(m.QueryOptimizer(), m.LFTJEngine(), m.dict)
	return executor.ExecuteDatalog(ctx, datalogQuery)
}

// CommunityDetector returns a new community detector for graph clustering.
func (m *MEBStore) CommunityDetector() *clustering.CommunityDetector {
	return clustering.NewCommunityDetector(m.db)
}

// DetectCommunities runs the Leiden algorithm to detect communities in a graph.
// This is a compute-intensive operation that should be run asynchronously in production.
func (m *MEBStore) DetectCommunities(graphID string) (*clustering.CommunityHierarchy, error) {
	detector := m.CommunityDetector()
	return detector.Detect(graphID)
}

// GetCommunityMembers returns all members of a specific community at a given level.
func (m *MEBStore) GetCommunityMembers(graphID string, level uint8, commID uint64) ([]uint64, error) {
	detector := m.CommunityDetector()
	return detector.GetCommunityMembers(graphID, level, commID)
}

// GetNodeCommunityPath returns the hierarchy path for a node (e.g., [L0, L1, L2]).
func (m *MEBStore) GetNodeCommunityPath(graphID string, nodeID uint64) ([]uint64, error) {
	detector := m.CommunityDetector()
	return detector.GetNodeCommunityPath(graphID, nodeID)
}

func (m *MEBStore) HybridClustering() *clustering.HybridClustering {
	return clustering.NewHybridClustering(m.db)
}

func (m *MEBStore) ClusterWithHybrid(
	graphID string,
	queryEmbedding []float32,
	limit int,
	numClusters int,
) (*clustering.HybridClusteringResult, error) {
	hc := m.HybridClustering()

	if err := hc.LoadCommunities(graphID); err != nil {
		return nil, err
	}

	vectorResults, err := m.Vectors().Search(queryEmbedding, limit)
	if err != nil {
		return nil, err
	}

	if len(vectorResults) == 0 {
		return nil, nil
	}

	nodeIDs := make([]uint64, len(vectorResults))
	for i, vr := range vectorResults {
		nodeIDs[i] = vr.ID
	}

	return hc.ClusterResults(vectorResults, nodeIDs, numClusters)
}

func (m *MEBStore) QueryOptimizer() *query.QueryOptimizer {
	return query.NewQueryOptimizer(m.db)
}

func (m *MEBStore) OptimizeQuery(datalogQuery string) (*query.QueryPlan, error) {
	optimizer := m.QueryOptimizer()
	return optimizer.OptimizeDatalogQuery(datalogQuery)
}

const (
	autoGCThreshold = 10000            // Run GC after every 10,000 facts
	minGCInterval   = 60 * time.Second // Minimum interval between GC runs
)

// triggerAutoGC runs garbage collection if thresholds are met
func (m *MEBStore) triggerAutoGC() {
	// Check if auto-GC is enabled
	if !m.config.EnableAutoGC {
		return
	}

	// Check if we've reached the threshold
	if m.factsSinceGC < autoGCThreshold {
		return
	}

	// Check if enough time has passed since last GC
	now := time.Now()
	if !m.lastGCTime.IsZero() && now.Sub(m.lastGCTime) < minGCInterval {
		return
	}

	// Run GC
	gcRatio := m.config.GCRatio
	if gcRatio <= 0 {
		gcRatio = 0.5
	}

	slog.Info("triggering auto-GC", "factsSinceGC", m.factsSinceGC, "ratio", gcRatio)

	// Run GC on facts DB
	if err := m.db.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
		slog.Warn("auto-GC failed for facts DB", "error", err)
	} else if err == nil {
		slog.Debug("auto-GC completed for facts DB")
	}

	// Reset counters
	m.factsSinceGC = 0
	m.lastGCTime = now
}

// === Temporal Facts API (DatalogMTL Support) ===

// EnableTemporal creates and initializes the temporal store for time-based facts.
// This must be called before using temporal fact methods.
func (m *MEBStore) EnableTemporal() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.temporalStore == nil {
		m.temporalStore = NewMEBTemporalStore()
	}
}

// IsTemporalEnabled returns true if temporal store is enabled.
func (m *MEBStore) IsTemporalEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.temporalStore != nil
}

// AddTemporalFact adds a fact that is valid during the specified time interval.
// This enables temporal queries like "who accessed doc_X in the last 30 days?"
func (m *MEBStore) AddTemporalFact(predicate, subject, object, graph string, validFrom, validTo time.Time) error {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return fmt.Errorf("temporal store not enabled. Call EnableTemporal() first")
	}

	// Convert to ast.Atom
	atom := ast.NewAtom(predicate,
		ast.Constant{Type: ast.StringType, Symbol: subject},
		ast.Constant{Type: ast.StringType, Symbol: object},
	)
	if graph != "" {
		atom = ast.NewAtom("triples",
			ast.Constant{Type: ast.StringType, Symbol: subject},
			ast.Constant{Type: ast.StringType, Symbol: predicate},
			ast.Constant{Type: ast.StringType, Symbol: object},
			ast.Constant{Type: ast.StringType, Symbol: graph},
		)
	}

	interval := ast.TimeInterval(validFrom, validTo)
	_, err := ts.Add(atom, interval)
	return err
}

// AddTemporalFactAt adds a fact that is valid at a specific point in time.
func (m *MEBStore) AddTemporalFactAt(predicate, subject, object, graph string, at time.Time) error {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return fmt.Errorf("temporal store not enabled. Call EnableTemporal() first")
	}

	atom := ast.NewAtom(predicate,
		ast.Constant{Type: ast.StringType, Symbol: subject},
		ast.Constant{Type: ast.StringType, Symbol: object},
	)
	if graph != "" {
		atom = ast.NewAtom("triples",
			ast.Constant{Type: ast.StringType, Symbol: subject},
			ast.Constant{Type: ast.StringType, Symbol: predicate},
			ast.Constant{Type: ast.StringType, Symbol: object},
			ast.Constant{Type: ast.StringType, Symbol: graph},
		)
	}

	interval := ast.NewPointInterval(at)
	_, err := ts.Add(atom, interval)
	return err
}

// AddEternalFact adds a fact that is valid for all time.
func (m *MEBStore) AddEternalFact(predicate, subject, object, graph string) error {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return fmt.Errorf("temporal store not enabled. Call EnableTemporal() first")
	}

	atom := ast.NewAtom(predicate,
		ast.Constant{Type: ast.StringType, Symbol: subject},
		ast.Constant{Type: ast.StringType, Symbol: object},
	)
	if graph != "" {
		atom = ast.NewAtom("triples",
			ast.Constant{Type: ast.StringType, Symbol: subject},
			ast.Constant{Type: ast.StringType, Symbol: predicate},
			ast.Constant{Type: ast.StringType, Symbol: object},
			ast.Constant{Type: ast.StringType, Symbol: graph},
		)
	}

	_, err := ts.AddEternal(atom)
	return err
}

// ContainsTemporalAt checks if a fact is valid at the specified time.
func (m *MEBStore) ContainsTemporalAt(predicate, subject, object, graph string, t time.Time) bool {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return false
	}

	atom := ast.NewAtom(predicate,
		ast.Constant{Type: ast.StringType, Symbol: subject},
		ast.Constant{Type: ast.StringType, Symbol: object},
	)
	if graph != "" {
		atom = ast.NewAtom("triples",
			ast.Constant{Type: ast.StringType, Symbol: subject},
			ast.Constant{Type: ast.StringType, Symbol: predicate},
			ast.Constant{Type: ast.StringType, Symbol: object},
			ast.Constant{Type: ast.StringType, Symbol: graph},
		)
	}

	return ts.ContainsAt(atom, t)
}

// GetTemporalFactsAt returns all facts valid at a specific time.
func (m *MEBStore) GetTemporalFactsAt(predicate string, t time.Time) ([]TemporalFact, error) {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return nil, fmt.Errorf("temporal store not enabled. Call EnableTemporal() first")
	}

	query := ast.NewQuery(ast.PredicateSym{Symbol: predicate})
	var results []TemporalFact

	err := ts.GetFactsAt(query, t, func(tf factstore.TemporalFact) error {
		results = append(results, TemporalFact{
			Fact: Fact{
				Subject:   tf.Atom.Args[0].String(),
				Predicate: tf.Atom.Predicate.Symbol,
				Object:    tf.Atom.Args[1].String(),
			},
			ValidFrom: tf.Interval.Start.Time(),
			ValidTo:   tf.Interval.End.Time(),
		})
		return nil
	})

	return results, err
}

// GetTemporalFactsDuring returns all facts valid during a time interval.
func (m *MEBStore) GetTemporalFactsDuring(predicate string, start, end time.Time) ([]TemporalFact, error) {
	m.mu.RLock()
	ts := m.temporalStore
	m.mu.RUnlock()

	if ts == nil {
		return nil, fmt.Errorf("temporal store not enabled. Call EnableTemporal() first")
	}

	query := ast.NewQuery(ast.PredicateSym{Symbol: predicate})
	interval := ast.TimeInterval(start, end)
	var results []TemporalFact

	err := ts.GetFactsDuring(query, interval, func(tf factstore.TemporalFact) error {
		results = append(results, TemporalFact{
			Fact: Fact{
				Subject:   tf.Atom.Args[0].String(),
				Predicate: tf.Atom.Predicate.Symbol,
				Object:    tf.Atom.Args[1].String(),
			},
			ValidFrom: tf.Interval.Start.Time(),
			ValidTo:   tf.Interval.End.Time(),
		})
		return nil
	})

	return results, err
}

// GetTemporalStore returns the underlying temporal store for advanced usage.
// Returns nil if temporal is not enabled.
func (m *MEBStore) GetTemporalStore() *MEBTemporalStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.temporalStore
}
