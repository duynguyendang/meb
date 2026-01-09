package predicates

import (
	"encoding/binary"
	"fmt"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/mangle/ast"
)

// PredicateTable implements streaming queries for a specific predicate.
type PredicateTable struct {
	db   *badger.DB
	pred ast.PredicateSym
	// prefix is the key prefix (SPOPrefix or OPSPrefix)
	prefix byte
}

// NewPredicateTable creates a new predicate table.
func NewPredicateTable(db *badger.DB, encoder dict.Dictionary, pred ast.PredicateSym, prefix byte) *PredicateTable {
	return &PredicateTable{
		db:     db,
		pred:   pred,
		prefix: prefix,
	}
}

// bindingInfo contains information about which arguments are bound.
type bindingInfo struct {
	// hasBound indicates if at least one argument is bound
	hasBound bool
	// boundPositions contains the indices of bound arguments
	boundPositions []int
	// boundValues contains the actual uint64 IDs for bound arguments
	boundValues []uint64
	// prefix is the seek prefix for BadgerDB iterator
	prefix []byte
}

// GetFacts streams facts matching the given atom using the callback.
// It implements strict binding checks - rejects full table scans.
func (pt *PredicateTable) GetFacts(atom ast.Atom, callback func(ast.Atom) error, encoder dict.Dictionary) error {
	// Analyze bindings
	bindings := pt.analyzeBindings(atom, encoder)

	// Reject full table scans
	if !bindings.hasBound && pt.pred.Arity > 0 {
		return fmt.Errorf("full table scan not allowed for predicate %s (at least one argument must be bound)", pt.pred.Symbol)
	}

	// Create Badger iterator
	txn := pt.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	opts.PrefetchValues = false // We only need keys

	it := txn.NewIterator(opts)
	defer it.Close()

	// Seek to the prefix
	it.Seek(bindings.prefix)

	// Stream results
	for it.ValidForPrefix(bindings.prefix) {
		item := it.Item()

		// Decode key to get IDs
		ids := pt.decodeKey(item.Key())

		// Check if this result matches the full binding pattern
		if !pt.matchesBindings(ids, bindings) {
			it.Next()
			continue
		}

		// Convert IDs to Constants using dictionary
		args := make([]ast.BaseTerm, len(ids))
		for i, id := range ids {
			if i < len(atom.Args) {
				// Check if this position was bound in the query
				if _, isVar := atom.Args[i].(ast.Variable); isVar {
					// Variable position - lookup string from dictionary
					s, err := encoder.GetString(id)
					if err != nil {
						// Skip this result if lookup fails
						it.Next()
						continue
					}
					args[i] = ast.Constant{
						Type:   ast.StringType,
						Symbol: s,
					}
				} else {
					// Bound position - use the original constant
					args[i] = atom.Args[i]
				}
			}
		}

		// Build result atom
		result := ast.Atom{
			Predicate: pt.pred,
			Args:      args,
		}

		// Yield to callback
		if err := callback(result); err != nil {
			return err
		}

		it.Next()
	}

	return nil
}

// analyzeBindings analyzes which arguments are bound (Constants) vs unbound (Variables).
func (pt *PredicateTable) analyzeBindings(atom ast.Atom, encoder dict.Dictionary) bindingInfo {
	info := bindingInfo{
		boundPositions: make([]int, 0),
		boundValues:    make([]uint64, 0),
	}

	// For SPO index, we check positions 0 (subject), 1 (predicate), 2 (object)
	for i := 0; i < len(atom.Args) && i < 3; i++ {
		if constTerm, ok := atom.Args[i].(ast.Constant); ok {
			info.hasBound = true
			info.boundPositions = append(info.boundPositions, i)

			// Convert string to ID
			id, err := encoder.GetOrCreateID(constTerm.Symbol)
			if err != nil {
				// If we can't get ID, use 0 (won't match anything)
				info.boundValues = append(info.boundValues, 0)
			} else {
				info.boundValues = append(info.boundValues, id)
			}
		}
	}

	// Build prefix based on bound arguments
	info.prefix = pt.buildPrefix(info)

	return info
}

// buildPrefix constructs the seek prefix for BadgerDB based on bound arguments.
// For SPO index:
// - If subject is bound: [0x01 | subjectID]
// - If subject+predicate bound: [0x01 | subjectID | predicateID]
// - If subject+predicate+object bound: [0x01 | subjectID | predicateID | objectID]
func (pt *PredicateTable) buildPrefix(info bindingInfo) []byte {
	// Start with prefix byte
	prefix := []byte{pt.prefix}

	// For SPO index, add bound values in order
	for i, pos := range info.boundPositions {
		if pos < 3 {
			// Add the bound ID
			idBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(idBytes, info.boundValues[i])
			prefix = append(prefix, idBytes...)
		}
	}

	return prefix
}

// decodeKey decodes a BadgerDB key back into uint64 IDs.
// Handles both SPO and OPS key formats.
func (pt *PredicateTable) decodeKey(key []byte) []uint64 {
	if len(key) < 25 {
		return nil
	}

	// Check prefix
	prefix := key[0]

	var subject, predicate, object uint64

	if prefix == keys.SPOPrefix {
		subject, predicate, object = keys.DecodeSPOKey(key)
	} else if prefix == keys.OPSPrefix {
		subject, predicate, object = keys.DecodeOPSKey(key)
	} else {
		return nil
	}

	return []uint64{subject, predicate, object}
}

// matchesBindings checks if the given IDs match the binding pattern.
func (pt *PredicateTable) matchesBindings(ids []uint64, bindings bindingInfo) bool {
	if len(ids) < 3 {
		return false
	}

	// Check each bound position
	for i, pos := range bindings.boundPositions {
		if pos < len(ids) {
			if ids[pos] != bindings.boundValues[i] {
				return false
			}
		}
	}

	return true
}
