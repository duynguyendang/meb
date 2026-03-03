package adapter

import (
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

// FactSource is the interface that the Mangle Inference Engine expects
// for querying facts from the EDB (Extensional Database) layer.
type FactSource interface {
	// Search returns an iterator for facts matching the given pattern.
	// predicate: the predicate symbol (e.g., "triples")
	// args: values for bound arguments, nil for unbound variables
	Search(predicate string, args []any) Iterator
}

// MebAdapter implements FactSource, bridging Mangle queries to MEBStore.
// It intelligently chooses between SPO and OPS indexes based on query patterns.
type MebAdapter struct {
	db   *badger.DB
	dict Dictionary
}

// NewMebAdapter creates a new MebAdapter.
func NewMebAdapter(db *badger.DB, dict Dictionary) *MebAdapter {
	return &MebAdapter{
		db:   db,
		dict: dict,
	}
}

// Search returns facts matching the pattern.
// It analyzes the query pattern and chooses the optimal index (SPO vs OPS).
//
// Examples:
//   - Search("triples", []any{"alice", nil, nil})   -> Subject bound -> Use SPO index
//   - Search("triples", []any{"alice", nil, "bob"}) -> Subject+Object bound -> Use SPO index
//   - Search("triples", []any{nil, nil, "bob"})     -> Object bound -> Use OPS index
func (a *MebAdapter) Search(predicate string, args []any) Iterator {
	// Validate predicate
	if predicate != "triples" {
		return &EmptyIterator{}
	}

	// Validate arity
	if len(args) != 3 {
		return &EmptyIterator{}
	}

	// Analyze binding pattern
	var sBound, pBound bool
	var sVal, pVal string

	if args[0] != nil {
		sBound = true
		sVal = strings.Trim(args[0].(string), "\"")
	}
	if args[1] != nil {
		pBound = true
		pVal = strings.Trim(args[1].(string), "\"")
	}
	// Object binding check
	var oBound bool
	var oVal string
	if args[2] != nil {
		oBound = true
		oVal = strings.Trim(args[2].(string), "\"")
	}

	// Reject full table scans
	if !sBound && !pBound && !oBound {
		return &EmptyIterator{}
	}

	// Strategy: Choose between SPO and OPS index
	var prefix []byte

	if sBound {
		// Use SPO Index
		sID, err := a.dict.GetID(sVal)
		if err != nil {
			return &EmptyIterator{}
		}

		var pID uint64
		if pBound {
			pID, err = a.dict.GetID(pVal)
			if err != nil {
				return &EmptyIterator{}
			}
		}

		// Use helper to generate SPOG quad prefix
		prefix = keys.EncodeQuadKey(keys.QuadSPOGPrefix, sID, pID, 0, 0)[:17] // S|P prefix

	} else if oBound {
		// Use OPSG Index (quad format)
		oID, err := a.dict.GetID(oVal)
		if err != nil {
			return &EmptyIterator{}
		}

		var pID uint64
		if pBound {
			pID, err = a.dict.GetID(pVal)
			if err != nil {
				return &EmptyIterator{}
			}
		}

		// Use helper to generate OPSG quad prefix
		if pID != 0 {
			prefix = keys.EncodeQuadOPSGPrefix(oID, pID, 0, 0)
		} else {
			prefix = keys.EncodeQuadOPSGPrefix(oID, 0, 0, 0)
		}

	} else {
		// Only Predicate bound?
		// We don't have a POS index.
		// Return empty iterator for now (inefficient to scan without S or O).
		return &EmptyIterator{}
	}

	// Create read-only transaction
	txn := a.db.NewTransaction(false)

	// Create iterator
	return NewBadgerIterator(txn, prefix, a.dict)
}
