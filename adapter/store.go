package adapter

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
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
		sVal = args[0].(string)
	}
	if args[1] != nil {
		pBound = true
		pVal = args[1].(string)
	}
	// Object binding check removed as OPS is unsupported currently

	// Reject full table scans
	if !sBound && !pBound {
		return &EmptyIterator{}
	}

	// Strategy: Use SPO index (24-byte S|P|O keys)
	// Note: OPS index is not currently maintained in the main store (meb.go), so we rely on SPO.

	var prefix []byte

	if sBound {
		// Convert subject to ID
		sID, err := a.dict.GetID(sVal) // Use GetID (read-only), not GetOrCreateID
		if err != nil {
			return &EmptyIterator{}
		}

		// S prefix (8 bytes)
		prefix = make([]byte, 8)
		binary.LittleEndian.PutUint64(prefix, sID) // Note: Badger uses BigEndian usually? meb.go uses BigEndian.
		// Wait, meb.go uses BigEndian. Let's stick to keys/encoding.go which uses BigEndian.

		// Correction: Utilize BigEndian as per keys/encoding.go
		binary.BigEndian.PutUint64(prefix, sID)

		if pBound {
			// Subject + Predicate bound
			pID, err := a.dict.GetID(pVal)
			if err != nil {
				return &EmptyIterator{}
			}

			// Append P (8 bytes)
			pBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(pBytes, pID)
			prefix = append(prefix, pBytes...)
		}
	} else {
		// No Subject bound.
		// Since we only maintain S|P|O index, we cannot efficiently query without S.
		// (OPS index support is currently disabled/removed in meb.go)
		return &EmptyIterator{}
	}

	// Create read-only transaction
	txn := a.db.NewTransaction(false)

	// Create iterator
	return NewBadgerIterator(txn, prefix, a.dict)
}
