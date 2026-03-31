package meb

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duynguyendang/meb/circuit"
	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/query"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"

	"github.com/dgraph-io/badger/v4"
)

type MEBStore struct {
	db     *badger.DB
	dictDB *badger.DB
	dict   dict.Dictionary

	config *store.Config
	mu     sync.RWMutex

	numFacts              atomic.Uint64
	factsSinceLastPersist atomic.Uint64
	factsSinceGC          atomic.Uint64

	vectors *vector.VectorRegistry
	breaker *circuit.Breaker

	// TopicID for symmetric bit-packing (24-bit, supports 16M topics)
	topicID atomic.Uint32

	// Default semantic hints applied to facts (can be overridden per fact)
	defaultEntityType uint16
	defaultFlags      uint16

	lastGCTimeNano atomic.Int64

	cleanupStop chan struct{}
	cleanupDone chan struct{}

	lftjEngine *query.LFTJEngine

	telemetry telemetryManager
	wal       *WAL
}

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

func (m *MEBStore) ensureSchemaVersion() error {
	var storedVersion uint64
	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keys.KeySchemaVersion)
		if err == badger.ErrKeyNotFound {
			storedVersion = 0
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			storedVersion = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		return err
	}

	if storedVersion == 0 {
		return m.withWriteTxn(func(txn *badger.Txn) error {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, keys.CurrentSchemaVersion)
			return txn.Set(keys.KeySchemaVersion, buf)
		})
	}

	if storedVersion != keys.CurrentSchemaVersion {
		return fmt.Errorf("schema version mismatch: stored=%d, current=%d", storedVersion, keys.CurrentSchemaVersion)
	}

	return nil
}

func (m *MEBStore) replayWAL() error {
	entries, err := m.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	slog.Info("replaying WAL", "entries", len(entries))

	facts := make([]Fact, 0, len(entries))
	for _, e := range entries {
		fact := Fact{
			Subject:   e.subject,
			Predicate: e.pred,
			Object:    e.object,
		}
		if fact.Object == nil && fact.Predicate != "" {
			fact.Object = ""
		}
		facts = append(facts, fact)
	}

	if len(facts) > 0 {
		if err := m.AddFactBatch(facts); err != nil {
			return fmt.Errorf("WAL replay AddFactBatch failed: %w", err)
		}
	}

	if err := m.wal.Clear(); err != nil {
		return fmt.Errorf("failed to clear WAL after replay: %w", err)
	}

	slog.Info("WAL replay complete", "factsReplayed", len(facts))
	return nil
}

func NewMEBStore(cfg *store.Config) (*MEBStore, error) {
	if cfg.Verbose {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}

	slog.Info("initializing MEB store",
		"dataDir", cfg.DataDir,
		"inMemory", cfg.InMemory,
	)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	db, err := store.OpenBadgerDB(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	slog.Info("BadgerDB (graph) opened successfully")

	dictCfg := *cfg
	dictCfg.DataDir = cfg.DictDir
	dictCfg.SyncWrites = true

	dictDB, err := store.OpenBadgerDB(&dictCfg)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open Dictionary BadgerDB: %w", err)
	}

	slog.Info("BadgerDB (Dictionary) opened successfully")

	dictEncoder, err := dict.NewEncoder(dictDB, cfg.LRUCacheSize, cfg.NumDictShards)
	if err != nil {
		dictDB.Close()
		db.Close()
		return nil, fmt.Errorf("failed to create dictionary encoder: %w", err)
	}

	vCfg := vector.DefaultConfig()
	vCfg.SegmentDir = cfg.SegmentDir

	wal, err := NewWAL(cfg.DataDir)
	if err != nil {
		db.Close()
		dictDB.Close()
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	m := &MEBStore{
		db:                db,
		dictDB:            dictDB,
		dict:              dictEncoder,
		config:            cfg,
		vectors:           vector.NewRegistry(db, vCfg),
		breaker:           circuit.NewBreaker(nil),
		defaultEntityType: keys.EntityUnknown,
		cleanupStop:       make(chan struct{}),
		cleanupDone:       make(chan struct{}),
		lftjEngine:        query.NewLFTJEngine(db),
		wal:               wal,
	}
	m.breaker.OnStateChange(func(oldState, newState circuit.State, err error) {
		m.telemetry.Emit("circuit_state_change", map[string]any{
			"oldState": oldState.String(),
			"newState": newState.String(),
			"error":    err,
		})
	})
	m.topicID.Store(1) // default topic

	if err := m.vectors.LoadSnapshot(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load vector snapshot: %w", err)
	}

	if err := m.loadStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load stats: %w", err)
	}

	if err := m.ensureSchemaVersion(); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema version check failed: %w", err)
	}

	if err := m.replayWAL(); err != nil {
		db.Close()
		return nil, fmt.Errorf("WAL replay failed: %w", err)
	}

	slog.Info("MEB store initialized successfully", "factCount", m.numFacts.Load())
	go m.runCleanupLoop()
	return m, nil
}

func (m *MEBStore) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Info("resetting store", "factCount", m.numFacts.Load())

	err := m.db.DropAll()
	if err != nil {
		return fmt.Errorf("failed to reset store: %w", err)
	}

	m.numFacts.Store(0)

	slog.Info("store reset complete")
	return nil
}

func (m *MEBStore) Close() error {
	slog.Info("closing store", "factCount", m.numFacts.Load())

	close(m.cleanupStop)
	<-m.cleanupDone

	var errs []error

	if !m.config.ReadOnly && m.config.EnableAutoGC {
		gcRatio := m.config.GCRatio
		if gcRatio <= 0 {
			gcRatio = 0.5
		}
		slog.Info("running GC before shutdown", "ratio", gcRatio)
		if err := m.db.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
			slog.Warn("GC failed for facts DB", "error", err)
		}
		if err := m.dictDB.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
			slog.Warn("GC failed for dictionary DB", "error", err)
		}
	}

	if !m.config.ReadOnly {
		if err := m.vectors.SaveSnapshot(); err != nil {
			slog.Error("failed to save vector snapshot", "error", err)
			errs = append(errs, fmt.Errorf("save vector snapshot: %w", err))
		}
		if err := m.saveStats(); err != nil {
			slog.Error("failed to save stats", "error", err)
			errs = append(errs, fmt.Errorf("save stats: %w", err))
		}
	}

	if err := m.vectors.Close(); err != nil {
		slog.Error("failed to close vectors", "error", err)
		errs = append(errs, fmt.Errorf("close vectors: %w", err))
	}

	if err := m.dict.Close(); err != nil {
		slog.Error("failed to close dictionary", "error", err)
		errs = append(errs, fmt.Errorf("close dictionary: %w", err))
	}

	if err := m.wal.Close(); err != nil {
		slog.Error("failed to close WAL", "error", err)
		errs = append(errs, fmt.Errorf("close WAL: %w", err))
	}

	if err := m.dictDB.Close(); err != nil {
		slog.Error("failed to close dictionary database", "error", err)
		errs = append(errs, fmt.Errorf("close dictionary database: %w", err))
	}

	if err := m.db.Close(); err != nil {
		slog.Error("failed to close database", "error", err)
		errs = append(errs, fmt.Errorf("close database: %w", err))
	}

	if len(errs) > 0 {
		slog.Warn("store closed with errors", "errorCount", len(errs))
		return fmt.Errorf("close completed with %d errors: %v", len(errs), errs)
	}

	slog.Info("store closed successfully")
	return nil
}

func (m *MEBStore) Count() uint64 {
	return m.numFacts.Load()
}

func (m *MEBStore) RecalculateStats() (uint64, error) {
	slog.Info("recalculating stats", "currentCount", m.numFacts.Load())

	var count uint64

	err := m.withReadTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{keys.TripleSPOPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if len(item.Key()) == keys.TripleKeySize {
				count++
			}
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to recalculate stats: %w", err)
	}

	m.numFacts.Store(count)

	if err := m.saveStats(); err != nil {
		return 0, fmt.Errorf("failed to save recalculated stats: %w", err)
	}

	slog.Info("stats recalculated successfully", "newCount", count)
	return count, nil
}

func (m *MEBStore) Vectors() *vector.VectorRegistry {
	return m.vectors
}

func (m *MEBStore) LFTJEngine() *query.LFTJEngine {
	return m.lftjEngine
}

func (m *MEBStore) Find() *Builder {
	return NewBuilder(m)
}

// SetTopicID sets the 24-bit topic ID for symmetric bit-packing.
// All facts added after this call will use the new topic ID.
// Supports up to 16M isolated namespaces (topics).
// Panics if topicID is 0 (reserved as invalid).
func (m *MEBStore) SetTopicID(topicID uint32) {
	topicID = topicID & 0xFFFFFF // clamp to 24 bits
	if topicID == 0 {
		panic("SetTopicID: topicID must be non-zero")
	}
	m.topicID.Store(topicID)
}

// TopicID returns the current topic ID.
func (m *MEBStore) TopicID() uint32 {
	return m.topicID.Load()
}

// SetDefaultEntityType sets the default entity type for semantic hints.
// Use keys.EntityFunc, EntityVar, EntityClass, etc.
func (m *MEBStore) SetDefaultEntityType(entityType uint16) {
	m.defaultEntityType = entityType & 0xF
}

// SetDefaultFlags sets the default flags for semantic hints.
// Use keys.FlagIsPublic, FlagIsDeprecated, FlagIsTest, FlagIsGenerated.
func (m *MEBStore) SetDefaultFlags(flags uint16) {
	m.defaultFlags = flags & 0xF
}

func (m *MEBStore) RunValueLogGC(ratio float64) error {
	if ratio <= 0 || ratio > 1 {
		ratio = 0.5
	}

	if err := m.db.RunValueLogGC(ratio); err != nil && err != badger.ErrNoRewrite {
		return fmt.Errorf("failed to run GC on facts DB: %w", err)
	}

	if err := m.dictDB.RunValueLogGC(ratio); err != nil && err != badger.ErrNoRewrite {
		return fmt.Errorf("failed to run GC on dictionary DB: %w", err)
	}

	return nil
}

func (m *MEBStore) CircuitBreaker() *circuit.Breaker {
	return m.breaker
}

func (m *MEBStore) SetCircuitBreakerConfig(config *circuit.Config) {
	m.breaker = circuit.NewBreaker(config)
}

func (m *MEBStore) CircuitBreakerMetrics() circuit.Metrics {
	return m.breaker.Metrics()
}

func (m *MEBStore) CircuitBreakerMetricsSnapshot() circuit.MetricsSnapshot {
	return m.breaker.MetricsSnapshot()
}

func (m *MEBStore) RegisterTelemetrySink(sink TelemetrySink) {
	m.telemetry.Register(sink)
}

func (m *MEBStore) UnregisterTelemetrySink(sink TelemetrySink) {
	m.telemetry.Unregister(sink)
}

func (m *MEBStore) ResolveID(id uint64) (string, error) {
	return m.dict.GetString(id)
}

const (
	autoGCThreshold       = 10000
	minGCInterval         = 60 * time.Second
	statsPersistThreshold = 5000
)

func (m *MEBStore) persistStatsIfNeeded(writesAdded uint64) {
	if m.config.ReadOnly {
		return
	}
	total := m.factsSinceLastPersist.Add(writesAdded)
	if total >= statsPersistThreshold {
		if m.factsSinceLastPersist.CompareAndSwap(total, 0) {
			if err := m.saveStats(); err != nil {
				slog.Debug("periodic stats persist failed", "error", err)
			}
		}
	}
}

func (m *MEBStore) triggerAutoGC() {
	if !m.config.EnableAutoGC {
		return
	}

	if m.factsSinceGC.Load() < autoGCThreshold {
		return
	}

	now := time.Now()
	lastNano := m.lastGCTimeNano.Load()
	if lastNano != 0 && now.Sub(time.Unix(0, lastNano)) < minGCInterval {
		return
	}

	gcRatio := m.config.GCRatio
	if gcRatio <= 0 {
		gcRatio = 0.5
	}

	slog.Info("triggering auto-GC", "factsSinceGC", m.factsSinceGC.Load(), "ratio", gcRatio)

	if err := m.db.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
		slog.Warn("auto-GC failed for facts DB", "error", err)
		m.telemetry.Emit("gc_failure", map[string]any{
			"db":    "facts",
			"error": err.Error(),
		})
	} else if err == nil {
		slog.Debug("auto-GC completed for facts DB")
	}

	m.factsSinceGC.Store(0)
	m.lastGCTimeNano.Store(now.UnixNano())
}

const cleanupInterval = 5 * time.Minute

const (
	defaultMaxFacts = 0 // 0 = unlimited
)

func (m *MEBStore) SetRetention(maxFacts uint64) {
	if maxFacts == 0 {
		return
	}
	current := m.numFacts.Load()
	if current <= maxFacts {
		return
	}
	slog.Info("retention policy triggered", "currentFacts", current, "maxFacts", maxFacts)
	m.telemetry.Emit("retention", map[string]any{
		"currentFacts": current,
		"maxFacts":     maxFacts,
		"excess":       current - maxFacts,
	})
}

func (m *MEBStore) runCleanupLoop() {
	defer close(m.cleanupDone)

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.cleanupStop:
			return
		case <-ticker.C:
			m.runCleanup()
		}
	}
}

func (m *MEBStore) runCleanup() {
	if m.config.ReadOnly {
		return
	}

	slog.Debug("running periodic cleanup")

	// Phase 1: Delete deprecated triples from disk
	deleted := m.cleanupDeprecatedTriples()
	if deleted > 0 {
		slog.Info("deprecated triples cleaned up", "count", deleted)
		m.telemetry.Emit("deprecated_cleanup", map[string]any{
			"count": deleted,
		})
	}

	// Phase 2: Run GC on both DBs
	if err := m.db.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		slog.Debug("periodic GC failed for facts DB", "error", err)
		m.telemetry.Emit("gc_failure", map[string]any{
			"db":    "facts",
			"error": err.Error(),
		})
	} else if err == nil {
		slog.Debug("periodic GC completed for facts DB")
	}

	if err := m.dictDB.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		slog.Debug("periodic GC failed for dictionary DB", "error", err)
		m.telemetry.Emit("gc_failure", map[string]any{
			"db":    "dictionary",
			"error": err.Error(),
		})
	} else if err == nil {
		slog.Debug("periodic GC completed for dictionary DB")
	}
}

func (m *MEBStore) cleanupDeprecatedTriples() int {
	const batchSize = 500
	totalDeleted := 0

	err := m.withWriteTxn(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{keys.TripleSPOPrefix}
		var keysToDelete [][]byte
		deletedInBatch := 0

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			if len(key) < keys.TripleKeySize {
				continue
			}

			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			if len(val) < 16 {
				continue
			}

			packed := binary.BigEndian.Uint64(val[8:16])
			hints := uint16(packed >> 48)
			_, _, flags := keys.DecodeSemanticHints(hints)

			if flags&keys.FlagIsDeprecated == 0 {
				continue
			}

			keysToDelete = append(keysToDelete, key)

			s, p, o := keys.DecodeTripleKey(key)
			opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, s, p, o)
			keysToDelete = append(keysToDelete, opsKey)

			deletedInBatch++
			totalDeleted++

			if deletedInBatch >= batchSize {
				for _, k := range keysToDelete {
					if err := txn.Delete(k); err != nil {
						return fmt.Errorf("failed to delete key: %w", err)
					}
				}
				keysToDelete = keysToDelete[:0]
				deletedInBatch = 0
			}
		}

		if len(keysToDelete) > 0 {
			for _, k := range keysToDelete {
				if err := txn.Delete(k); err != nil {
					return fmt.Errorf("failed to delete key: %w", err)
				}
			}
		}

		return nil
	})

	if err != nil {
		slog.Warn("deprecated cleanup failed", "error", err)
		m.telemetry.Emit("deprecated_cleanup_failed", map[string]any{
			"error": err.Error(),
		})
	}

	if totalDeleted > 0 {
		m.numFacts.Add(^uint64(totalDeleted - 1))
	}

	return totalDeleted
}
