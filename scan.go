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
	index  byte // SPOPrefix, OPSPrefix, or PSOPrefix
}

// selectScanStrategy determines the best index and prefix for a scan operation.
// Returns error if no efficient scan strategy exists (e.g., no arguments bound).
func selectScanStrategy(sBound, pBound, oBound bool, sID, pID, oID uint64) (*scanStrategy, error) {
	if !sBound && !pBound && !oBound {
		// No arguments bound - allow full scan if caller explicitly used empty strings
	}

	var strategy scanStrategy

	if sBound {
		// Use SPO index with S or S|P prefix
		strategy.prefix = keys.EncodeSPOPrefix(sID, pID)
		strategy.index = keys.SPOPrefix
	} else if oBound {
		// Use OPS index with O or O|P prefix
		strategy.prefix = keys.EncodeOPSPrefix(oID, pID)
		strategy.index = keys.OPSPrefix
	} else if pBound {
		// Use PSO index with P prefix
		strategy.prefix = keys.EncodePSOPrefix(pID, sID)
		strategy.index = keys.PSOPrefix
	} else {
		// No arguments bound - fallback to SPO scan for all facts
		strategy.prefix = []byte{keys.SPOPrefix}
		strategy.index = keys.SPOPrefix
	}

	return &strategy, nil
}

// resolveScanIDs resolves the bound string arguments to their dictionary IDs.
// Returns early if any string is not found in the dictionary.
func (m *MEBStore) resolveScanIDs(s, p, o string) (sID, pID, oID uint64, sBound, pBound, oBound bool, err error) {
	if s != "" {
		sID, err = m.dict.GetID(s)
		if err != nil {
			return 0, 0, 0, false, false, false, fmt.Errorf("subject not found: %w", err)
		}
		sBound = true
	}

	if p != "" {
		pID, err = m.dict.GetID(p)
		if err != nil {
			return 0, 0, 0, false, false, false, fmt.Errorf("predicate not found: %w", err)
		}
		pBound = true
	}

	if o != "" {
		oID, err = m.dict.GetID(o)
		if err != nil {
			return 0, 0, 0, false, false, false, fmt.Errorf("object not found: %w", err)
		}
		oBound = true
	}

	return sID, pID, oID, sBound, pBound, oBound, nil
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
// Optimized for 24-byte S|P|O keys with intelligent index selection.
func (m *MEBStore) ScanContext(ctx context.Context, s, p, o, g string) iter.Seq2[Fact, error] {
	return func(yield func(Fact, error) bool) {
		// Resolve bound string arguments to IDs
		sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
		if err != nil {
			// String not found in dictionary -> no matching facts
			return
		}

		// Select optimal scan strategy (index and prefix)
		strategy, err := selectScanStrategy(sBound, pBound, oBound, sID, pID, oID)
		if err != nil {
			// No efficient scan possible (e.g., no arguments bound)
			return
		}

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

			if len(key) != keys.TripleKeySize { // 25 bytes
				continue // Skip non-fact keys
			}

			// Decode the key based on which index we're using
			var foundSID, foundPID, foundOID uint64
			switch strategy.index {
			case keys.SPOPrefix:
				foundSID, foundPID, foundOID = keys.DecodeSPOKey(key)
			case keys.OPSPrefix:
				foundSID, foundPID, foundOID = keys.DecodeOPSKey(key)
			case keys.PSOPrefix:
				foundSID, foundPID, foundOID = keys.DecodePSOKey(key)
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

			// Build the Fact
			fact := Fact{
				Subject:   subject,
				Predicate: predicate,
				Object:    objectStr,
				Graph:     normalizeGraph(g), // Use normalized graph
			}

			if !yield(fact, nil) {
				return
			}
		}
	}
}
