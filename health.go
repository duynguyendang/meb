package meb

import (
	"fmt"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// HealthStatus summarizes the durability and resource state of the store for
// monitoring/health endpoints.
type HealthStatus struct {
	StoreOpen    bool   // false once the underlying Badger DB is closed
	ReadOnly     bool   // true when the store is opened in read-only mode
	NumFacts     uint64 // in-memory triple count
	NumVectors   int    // in-memory vector registry count
	LSMSize      int64  // LSM tree size on disk (bytes)
	ValueLogSize int64  // value log size on disk (bytes)
	DiskUsage    int64  // LSMSize + ValueLogSize (bytes)
	MemoryInUse  uint64 // Go heap currently in use (bytes)
	LastGCAt     time.Time
}

// Health returns a snapshot of the store's health. It is safe to call at any
// time (including after Close); values simply reflect the closed state.
func (m *MEBStore) Health() HealthStatus {
	h := HealthStatus{
		StoreOpen:  m.db != nil && !m.db.IsClosed(),
		ReadOnly:   m.config.ReadOnly,
		NumFacts:   m.numFacts.Load(),
		NumVectors: m.vectors.Count(),
		LastGCAt:   time.Unix(0, m.lastGCTimeNano.Load()),
	}
	if m.db != nil {
		h.LSMSize, h.ValueLogSize = m.db.Size()
		h.DiskUsage = h.LSMSize + h.ValueLogSize
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	h.MemoryInUse = ms.Alloc
	return h
}

// Compact runs Badger value-log garbage collection, rewriting stale values to
// reclaim disk space. ratio is the fraction of a value log file's bytes that
// must be discardable to justify rewriting it (0-1; badger.ErrNoRewrite is
// returned when there is nothing to reclaim). The store's configured GCRatio is
// used if ratio is <= 0.
func (m *MEBStore) Compact(ratio float64) error {
	if m.db == nil {
		return ErrStoreClosed
	}
	if m.db.IsClosed() {
		return ErrStoreClosed
	}
	if ratio <= 0 {
		ratio = m.config.GCRatio
	}
	if ratio <= 0 {
		ratio = 0.5
	}
	if err := m.db.RunValueLogGC(ratio); err != nil {
		if err == badger.ErrNoRewrite {
			return err
		}
		return fmt.Errorf("Compact: %w", err)
	}
	return nil
}
