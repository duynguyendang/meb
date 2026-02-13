package adapter

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/google/mangle/ast"
)

// Iterator defines the interface for streaming facts from the EDB layer.
// This is the contract that the Mangle Inference Engine expects.
type Iterator interface {
	// Next advances the iterator and returns the next Atom, or false if exhausted.
	Next() (ast.Atom, bool)
	// Close releases any resources held by the iterator.
	Close() error
}

// EmptyIterator is an iterator that always returns false (no results).
// Used for queries that can't be satisfied (e.g., unknown predicate).
type EmptyIterator struct{}

// Next returns false immediately, indicating no results.
func (ei *EmptyIterator) Next() (ast.Atom, bool) {
	return ast.Atom{}, false
}

// Close is a no-op for EmptyIterator.
func (ei *EmptyIterator) Close() error {
	return nil
}

// BadgerIterator wraps a BadgerDB iterator to conform to the Iterator interface.
// It handles decoding keys from BadgerDB format (SPO or OPS) into Atoms.
type BadgerIterator struct {
	txn    *badger.Txn
	it     *badger.Iterator
	prefix []byte
	closed bool

	// Configuration
	decodeStrings bool // If true, decode IDs to strings (lazy decoding)

	// For string decoding
	dict Dictionary
}

// Dictionary is the interface for bidirectional string/ID conversion.
type Dictionary interface {
	GetString(id uint64) (string, error)
	GetID(s string) (uint64, error)
	GetOrCreateID(s string) (uint64, error)
}

// NewBadgerIterator creates a new BadgerIterator.
func NewBadgerIterator(txn *badger.Txn, prefix []byte, dict Dictionary) *BadgerIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	opts.PrefetchValues = false // We only need keys

	it := txn.NewIterator(opts)

	return &BadgerIterator{
		txn:           txn,
		it:            it,
		prefix:        prefix,
		decodeStrings: true, // Enable lazy decoding by default
		dict:          dict,
	}
}

// Next advances the iterator and returns the next Atom.
// It decodes the BadgerDB key and converts it to a Mangle Atom.
func (bit *BadgerIterator) Next() (ast.Atom, bool) {
	if bit.closed {
		return ast.Atom{}, false
	}

	// Seek to prefix on first call
	if !bit.it.Valid() {
		bit.it.Seek(bit.prefix)
	}

	// Check if we're still within the prefix
	if !bit.it.ValidForPrefix(bit.prefix) {
		return ast.Atom{}, false
	}

	// Get the key
	item := bit.it.Item()
	key := item.Key()

	// Decode the key based on its prefix
	s, p, o, err := bit.decodeKey(key)
	if err != nil {
		bit.it.Next()
		return ast.Atom{}, false
	}

	// Build the Atom
	atom := ast.Atom{
		Predicate: ast.PredicateSym{
			Symbol: "triples",
			Arity:  3,
		},
		Args: []ast.BaseTerm{
			ast.Constant{Type: ast.StringType, Symbol: s},
			ast.Constant{Type: ast.StringType, Symbol: p},
			ast.Constant{Type: ast.StringType, Symbol: o},
		},
	}

	bit.it.Next()
	return atom, true
}

// Close closes the iterator and releases the transaction.
func (bit *BadgerIterator) Close() error {
	if bit.closed {
		return nil
	}
	bit.closed = true
	bit.it.Close()
	bit.txn.Discard()
	return nil
}

// decodeKey decodes a BadgerDB quad key back into subject, predicate, object strings.
// MEB v2 only supports 33-byte quad keys (SPOG format).
func (bit *BadgerIterator) decodeKey(key []byte) (subject, predicate, object string, err error) {
	// Only support 33-byte quad keys
	if len(key) != keys.QuadKeySize {
		return "", "", "", nil
	}

	// Decode quad key (ignore graph ID for triples adapter)
	sID, pID, oID, _ := keys.DecodeQuadKey(key)

	// Lazy decode: only convert to strings if decodeStrings is enabled
	if bit.decodeStrings && bit.dict != nil {
		s, err := bit.dict.GetString(sID)
		if err != nil {
			return "", "", "", err
		}

		p, err := bit.dict.GetString(pID)
		if err != nil {
			return "", "", "", err
		}

		o, err := bit.dict.GetString(oID)
		if err != nil {
			return "", "", "", err
		}

		return s, p, o, nil
	}

	// Return ID placeholders if not decoding strings
	return uint64ToString(sID), uint64ToString(pID), uint64ToString(oID), nil
}

// decodeUint64 decodes a big-endian uint64 from bytes.
func decodeUint64(b []byte) uint64 {
	_ = b[7] // bounds check hint
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

// uint64ToString converts a uint64 to a string placeholder (for zero-decode mode).
func uint64ToString(id uint64) string {
	return "id:" + string(rune(id))
}
