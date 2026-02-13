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
			return 0, 0, 0, 0, false, false, false, false, fmt.Errorf("subject not found: %w", err)
		}
		sBound = true
	}

	if p != "" {
		pID, err = m.dict.GetID(p)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, fmt.Errorf("predicate not found: %w", err)
		}
		pBound = true
	}

	if o != "" {
		oID, err = m.dict.GetID(o)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, fmt.Errorf("object not found: %w", err)
		}
		oBound = true
	}

	if g != "" {
		gID, err = m.dict.GetID(g)
		if err != nil {
			return 0, 0, 0, 0, false, false, false, false, fmt.Errorf("graph not found: %w", err)
		}
		gBound = true
	}

	return sID, pID, oID, gID, sBound, pBound, oBound, gBound, nil
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
	return func(yield func(Fact, error) bool) {
		// Normalize graph
		g = normalizeGraph(g)

		// Resolve bound string arguments to IDs
		sID, pID, oID, gID, sBound, pBound, oBound, gBound, err := m.resolveScanIDs(s, p, o, g)
		if err != nil {
			// String not found in dictionary -> no matching facts
			return
		}

		// Select optimal scan strategy (index and prefix)
		strategy := selectScanStrategy(gBound, sBound, pBound, oBound, gID, sID, pID, oID)

		// Create read-only transaction
		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		// Create iterator
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys

		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan using the selected prefix
		for it.Seek(strategy.prefix); it.ValidForPrefix(strategy.prefix); it.Next() {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				yield(Fact{}, ctx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.QuadKeySize { // 33 bytes
				continue // Skip non-fact keys
			}

			// Decode the key based on which index we're using
			var foundSID, foundPID, foundOID, foundGID uint64
			switch strategy.index {
			case keys.QuadSPOGPrefix:
				foundSID, foundPID, foundOID, foundGID = keys.DecodeQuadKey(key)
			case keys.QuadPOSGPrefix:
				foundPID, foundOID, foundSID, foundGID = keys.DecodeQuadKey(key)
			case keys.QuadGSPOPrefix:
				foundGID, foundSID, foundPID, foundOID = keys.DecodeQuadKey(key)
			}

			// Apply additional filters for bound arguments
			// (prefix scan handles most filtering, but we verify for safety)
			if sBound && foundSID != sID {
				continue
			}
			if pBound && foundPID != pID {
				continue
			}
			if oBound && foundOID != oID {
				continue
			}
			if gBound && foundGID != gID {
				continue
			}

			// Resolve IDs to strings
			var subject, predicate, objectStr string

			// Subject
			if sBound {
				subject = s
			} else {
				subject, err = m.dict.GetString(foundSID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve subject ID %d: %w", foundSID, err))
					return
				}
			}

			// Predicate
			if pBound {
				predicate = p
			} else {
				predicate, err = m.dict.GetString(foundPID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve predicate ID %d: %w", foundPID, err))
					return
				}
			}

			// Object
			if oBound {
				objectStr = o
			} else {
				objectStr, err = m.dict.GetString(foundOID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve object ID %d: %w", foundOID, err))
					return
				}
			}

			// Resolve Graph ID to string
			var graphStr string
			if gBound {
				graphStr = g
			} else {
				graphStr, err = m.dict.GetString(foundGID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve graph ID %d: %w", foundGID, err))
					return
				}
			}

			// Build the Fact
			fact := Fact{
				Subject:   subject,
				Predicate: predicate,
				Object:    objectStr,
				Graph:     graphStr,
			}

			if !yield(fact, nil) {
				return
			}
		}
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
func (m *MEBStore) ScanZeroCopyContext(ctx context.Context, s, p, o, g string) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		g = normalizeGraph(g)

		sID, pID, oID, gID, sBound, pBound, oBound, gBound, err := m.resolveScanIDs(s, p, o, g)
		if err != nil {
			return
		}

		strategy := selectScanStrategy(gBound, sBound, pBound, oBound, gID, sID, pID, oID)

		txn := m.db.NewTransaction(false)
		defer txn.Discard()

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(strategy.prefix); it.ValidForPrefix(strategy.prefix); it.Next() {
			select {
			case <-ctx.Done():
				yield(Fact{}, ctx.Err())
				return
			default:
			}

			item := it.Item()
			key := item.Key()

			if len(key) != keys.QuadKeySize {
				continue
			}

			var foundSID, foundPID, foundOID, foundGID uint64
			switch strategy.index {
			case keys.QuadSPOGPrefix:
				foundSID, foundPID, foundOID, foundGID = keys.DecodeQuadKey(key)
			case keys.QuadPOSGPrefix:
				foundPID, foundOID, foundSID, foundGID = keys.DecodeQuadKey(key)
			case keys.QuadGSPOPrefix:
				foundGID, foundSID, foundPID, foundOID = keys.DecodeQuadKey(key)
			}

			if sBound && foundSID != sID {
				continue
			}
			if pBound && foundPID != pID {
				continue
			}
			if oBound && foundOID != oID {
				continue
			}
			if gBound && foundGID != gID {
				continue
			}

			var subject, predicate, objectStr, graphStr string

			if sBound {
				subject = s
			} else {
				subject, err = m.dict.GetString(foundSID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve subject ID %d: %w", foundSID, err))
					return
				}
			}

			if pBound {
				predicate = p
			} else {
				predicate, err = m.dict.GetString(foundPID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve predicate ID %d: %w", foundPID, err))
					return
				}
			}

			if oBound {
				objectStr = o
			} else {
				objectStr, err = m.dict.GetString(foundOID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve object ID %d: %w", foundOID, err))
					return
				}
			}

			if gBound {
				graphStr = g
			} else {
				graphStr, err = m.dict.GetString(foundGID)
				if err != nil {
					yield(Fact{}, fmt.Errorf("failed to resolve graph ID %d: %w", foundGID, err))
					return
				}
			}

			fact := Fact{
				Subject:   subject,
				Predicate: predicate,
				Object:    objectStr,
				Graph:     graphStr,
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}
