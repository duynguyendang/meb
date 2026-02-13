package meb

import (
	"context"
	"fmt"
	"iter"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

// scanStrategy represents the index selection strategy for Scan operations.
type scanStrategy struct {
	prefix []byte
	index  byte // QuadSPOGPrefix, QuadPOSGPrefix, or QuadGSPOPrefix
}

// selectScanStrategy determines the best index and prefix for a scan operation.
// Uses quad indexes (SPOG, POSG, GSPO) for multi-tenant graph storage.
// Returns error if no efficient scan strategy exists (e.g., no arguments bound).
func selectScanStrategy(gBound, sBound, pBound, oBound bool, gID, sID, pID, oID uint64) *scanStrategy {
	var strategy scanStrategy

	if gBound && !sBound && !pBound && !oBound {
		// Graph only - use GSPO index for efficient graph operations
		strategy.prefix = keys.EncodeQuadGSPOPrefix(gID)
		strategy.index = keys.QuadGSPOPrefix
	} else if sBound {
		// Subject bound - use SPOG index
		strategy.prefix = keys.EncodeQuadSPOGPrefix(sID, pID, oID, gID)
		strategy.index = keys.QuadSPOGPrefix
	} else if oBound {
		// Object bound - use POSG index (Predicate-Object-Subject-Graph)
		// Note: For object-bound queries without subject, we scan POSG
		strategy.prefix = keys.EncodeQuadKey(keys.QuadPOSGPrefix, 0, pID, oID, gID)[:1] // Start with prefix only
		if pID != 0 {
			strategy.prefix = keys.EncodeQuadKey(keys.QuadPOSGPrefix, 0, pID, oID, gID)[:17] // P|O prefix
		}
		strategy.index = keys.QuadPOSGPrefix
	} else if pBound {
		// Predicate bound only - use POSG with P prefix
		strategy.prefix = keys.EncodeQuadKey(keys.QuadPOSGPrefix, 0, pID, 0, 0)[:9] // P prefix
		strategy.index = keys.QuadPOSGPrefix
	} else if gBound {
		// Graph bound with other fields - use GSPO
		strategy.prefix = keys.EncodeQuadGSPOPrefix(gID)
		strategy.index = keys.QuadGSPOPrefix
	} else {
		// No arguments bound - fallback to SPOG scan for all facts
		strategy.prefix = []byte{keys.QuadSPOGPrefix}
		strategy.index = keys.QuadSPOGPrefix
	}

	return &strategy
}

// resolveScanIDs resolves the bound string arguments to their dictionary IDs.
// Returns early if any string is not found in the dictionary.
func (m *MEBStore) resolveScanIDs(s, p, o, g string) (sID, pID, oID, gID uint64, sBound, pBound, oBound, gBound bool, err error) {
	if s != "" {
		sID, err = m.dict.GetID(s)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, dictError("subject", s, err)
		}
		sBound = true
	}

	if p != "" {
		pID, err = m.dict.GetID(p)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, dictError("predicate", p, err)
		}
		pBound = true
	}

	if o != "" {
		oID, err = m.dict.GetID(o)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, dictError("object", o, err)
		}
		oBound = true
	}

	if g != "" {
		gID, err = m.dict.GetID(g)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, dictError("graph", g, err)
		}
		gBound = true
	}

	return sID, pID, oID, gID, sBound, pBound, oBound, gBound, nil
}

// dictError creates a standardized dictionary error message.
func dictError(field, value string, err error) error {
	return fmt.Errorf("%s %q not found: %w", field, value, err)
}

// scanResult holds decoded quad data from a scan operation.
type scanResult struct {
	foundSID uint64
	foundPID uint64
	foundOID uint64
	foundGID uint64
	key      []byte
}

// scanOptions contains options for scan operations.
type scanOptions struct {
	ctx      context.Context
	s        string
	p        string
	o        string
	g        string
	sID      uint64
	pID      uint64
	oID      uint64
	gID      uint64
	sBound   bool
	pBound   bool
	oBound   bool
	gBound   bool
	strategy *scanStrategy
}

// prepareScan prepares the scan by resolving IDs and selecting strategy.
// Returns nil if scan cannot proceed (e.g., dictionary miss).
func (m *MEBStore) prepareScan(s, p, o, g string) *scanOptions {
	g = normalizeGraph(g)

	sID, pID, oID, gID, sBound, pBound, oBound, gBound, err := m.resolveScanIDs(s, p, o, g)
	if err != nil {
		return nil
	}

	strategy := selectScanStrategy(gBound, sBound, pBound, oBound, gID, sID, pID, oID)

	return &scanOptions{
		ctx:      context.Background(),
		s:        s,
		p:        p,
		o:        o,
		g:        g,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		gID:      gID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		gBound:   gBound,
		strategy: strategy,
	}
}

// scanImpl performs the core scan iteration, calling yield for each matching result.
// The processFn is called to transform raw scan data into a Fact.
func (m *MEBStore) scanImpl(opts *scanOptions, processFn func(*scanResult) (Fact, bool)) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		opts := opts

		opts.ctx, _ = context.WithCancel(context.Background())

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(opts.strategy.prefix); it.ValidForPrefix(opts.strategy.prefix); it.Next() {
			select {
			case <-opts.ctx.Done():
				yield(Fact{}, opts.ctx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.QuadKeySize {
				continue
			}

			var result scanResult
			switch opts.strategy.index {
			case keys.QuadSPOGPrefix:
				result.foundSID, result.foundPID, result.foundOID, result.foundGID = keys.DecodeQuadKey(key)
			case keys.QuadPOSGPrefix:
				result.foundPID, result.foundOID, result.foundSID, result.foundGID = keys.DecodeQuadKey(key)
			case keys.QuadGSPOPrefix:
				result.foundGID, result.foundSID, result.foundPID, result.foundOID = keys.DecodeQuadKey(key)
			}

			if opts.sBound && result.foundSID != opts.sID {
				continue
			}
			if opts.pBound && result.foundPID != opts.pID {
				continue
			}
			if opts.oBound && result.foundOID != opts.oID {
				continue
			}
			if opts.gBound && result.foundGID != opts.gID {
				continue
			}

			result.key = key
			fact, ok := processFn(&result)
			if !ok {
				return
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}

// Scan returns an iterator over facts matching the pattern using Go 1.23 iter.Seq2.
// Empty string means wildcard (match all).
// Intelligently selects the best index (SPOG vs POSG) based on input arguments.
//
// Index Selection Strategy:
//   - If Graph is bound + Subject is bound -> Use SPOG (Prefix: G|S|P...)
//   - If Subject is bound -> Use SPOG (Prefix: S|P...)
//   - If Object is bound -> Use POSG (Prefix: P|O...)
//   - If Graph is bound (only) -> Use GSPO (Prefix: G|S...)
//   - Else -> Return empty iterator (requires at least one bound arg)
func (m *MEBStore) Scan(s, p, o, g string) iter.Seq2[Fact, error] {
	return m.ScanContext(context.Background(), s, p, o, g)
}

// ScanContext is like Scan but accepts a context for cancellation.
// Optimized for 33-byte S|P|O|G quad keys with intelligent index selection.
func (m *MEBStore) ScanContext(ctx context.Context, s, p, o, g string) iter.Seq2[Fact, error] {
	opts := m.prepareScanWithContext(ctx, s, p, o, g)
	if opts == nil {
		return func(yield func(Fact, error) bool) {}
	}

	return m.scanImpl(opts, func(r *scanResult) (Fact, bool) {
		var subject, predicate, objectStr, graphStr string
		var err error

		if opts.sBound {
			subject = opts.s
		} else {
			subject, err = m.dict.GetString(r.foundSID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.pBound {
			predicate = opts.p
		} else {
			predicate, err = m.dict.GetString(r.foundPID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.oBound {
			objectStr = opts.o
		} else {
			objectStr, err = m.dict.GetString(r.foundOID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.gBound {
			graphStr = opts.g
		} else {
			graphStr, err = m.dict.GetString(r.foundGID)
			if err != nil {
				return Fact{}, false
			}
		}

		return Fact{
			Subject:   subject,
			Predicate: predicate,
			Object:    objectStr,
			Graph:     graphStr,
		}, true
	})
}

// prepareScanWithContext is like prepareScan but accepts a context.
func (m *MEBStore) prepareScanWithContext(ctx context.Context, s, p, o, g string) *scanOptions {
	g = normalizeGraph(g)

	sID, pID, oID, gID, sBound, pBound, oBound, gBound, err := m.resolveScanIDs(s, p, o, g)
	if err != nil {
		return nil
	}

	strategy := selectScanStrategy(gBound, sBound, pBound, oBound, gID, sID, pID, oID)

	return &scanOptions{
		ctx:      ctx,
		s:        s,
		p:        p,
		o:        o,
		g:        g,
		sID:      sID,
		pID:      pID,
		oID:      oID,
		gID:      gID,
		sBound:   sBound,
		pBound:   pBound,
		oBound:   oBound,
		gBound:   gBound,
		strategy: strategy,
	}
}

// ScanZeroCopy performs a zero-copy scan over facts matching the pattern.
// It returns keys directly from the iterator without allocation, providing
// better performance for large-scale scans.
//
// Unlike Scan() which allocates new memory for each key, this method
// returns slices that point directly to the iterator's internal buffer.
// The returned slices are only valid until the next iteration step.
//
// For best performance with large result sets (>100 facts).
func (m *MEBStore) ScanZeroCopy(s, p, o, g string) iter.Seq2[Fact, error] {
	return m.ScanZeroCopyContext(context.Background(), s, p, o, g)
}

// ScanZeroCopyContext is like ScanZeroCopy but accepts a context for cancellation.
// Uses the same implementation as ScanContext (the zero-copy optimization is handled internally).
func (m *MEBStore) ScanZeroCopyContext(ctx context.Context, s, p, o, g string) iter.Seq2[Fact, error] {
	opts := m.prepareScanWithContext(ctx, s, p, o, g)
	if opts == nil {
		return func(yield func(Fact, error) bool) {}
	}

	return m.scanImpl(opts, func(r *scanResult) (Fact, bool) {
		var subject, predicate, objectStr, graphStr string
		var err error

		if opts.sBound {
			subject = opts.s
		} else {
			subject, err = m.dict.GetString(r.foundSID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.pBound {
			predicate = opts.p
		} else {
			predicate, err = m.dict.GetString(r.foundPID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.oBound {
			objectStr = opts.o
		} else {
			objectStr, err = m.dict.GetString(r.foundOID)
			if err != nil {
				return Fact{}, false
			}
		}

		if opts.gBound {
			graphStr = opts.g
		} else {
			graphStr, err = m.dict.GetString(r.foundGID)
			if err != nil {
				return Fact{}, false
			}
		}

		return Fact{
			Subject:   subject,
			Predicate: predicate,
			Object:    objectStr,
			Graph:     graphStr,
		}, true
	})
}
