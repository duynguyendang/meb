package meb

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

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
