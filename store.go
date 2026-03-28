package meb

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duynguyendang/meb/circuit"
	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
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

	vectors *vector.VectorRegistry
	breaker *circuit.Breaker

	lastGCTime   time.Time
	factsSinceGC uint64
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

func NewMEBStore(cfg *store.Config) (*MEBStore, error) {
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

	dictEncoder, err := dict.NewEncoder(dictDB, cfg.LRUCacheSize)
	if err != nil {
		dictDB.Close()
		db.Close()
		return nil, fmt.Errorf("failed to create dictionary encoder: %w", err)
	}

	m := &MEBStore{
		db:      db,
		dictDB:  dictDB,
		dict:    dictEncoder,
		config:  cfg,
		vectors: vector.NewRegistry(db, nil),
		breaker: circuit.NewBreaker(nil),
	}

	if err := m.vectors.LoadSnapshot(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load vector snapshot: %w", err)
	}

	if err := m.loadStats(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load stats: %w", err)
	}

	slog.Info("MEB store initialized successfully", "factCount", m.numFacts.Load())
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

func (m *MEBStore) Find() *Builder {
	return NewBuilder(m)
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

	if m.factsSinceGC < autoGCThreshold {
		return
	}

	now := time.Now()
	if !m.lastGCTime.IsZero() && now.Sub(m.lastGCTime) < minGCInterval {
		return
	}

	gcRatio := m.config.GCRatio
	if gcRatio <= 0 {
		gcRatio = 0.5
	}

	slog.Info("triggering auto-GC", "factsSinceGC", m.factsSinceGC, "ratio", gcRatio)

	if err := m.db.RunValueLogGC(gcRatio); err != nil && err != badger.ErrNoRewrite {
		slog.Warn("auto-GC failed for facts DB", "error", err)
	} else if err == nil {
		slog.Debug("auto-GC completed for facts DB")
	}

	m.factsSinceGC = 0
	m.lastGCTime = now
}
