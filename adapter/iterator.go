package adapter

import (
	"codeberg.org/TauCeti/mangle-go/ast"
	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type Iterator interface {
	Next() (ast.Atom, bool)
	Close() error
}

type EmptyIterator struct{}

func (ei *EmptyIterator) Next() (ast.Atom, bool) {
	return ast.Atom{}, false
}

func (ei *EmptyIterator) Close() error {
	return nil
}

type BadgerIterator struct {
	txn    *badger.Txn
	it     *badger.Iterator
	prefix []byte
	closed bool

	decodeStrings bool
	dict          Dictionary
}

type Dictionary interface {
	GetString(id uint64) (string, error)
	GetID(s string) (uint64, error)
	GetOrCreateID(s string) (uint64, error)
}

func NewBadgerIterator(txn *badger.Txn, prefix []byte, dict Dictionary) *BadgerIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	return &BadgerIterator{
		txn:           txn,
		it:            it,
		prefix:        prefix,
		decodeStrings: true,
		dict:          dict,
	}
}

func (bit *BadgerIterator) Next() (ast.Atom, bool) {
	if bit.closed {
		return ast.Atom{}, false
	}

	if !bit.it.Valid() {
		bit.it.Seek(bit.prefix)
	}

	if !bit.it.ValidForPrefix(bit.prefix) {
		return ast.Atom{}, false
	}

	item := bit.it.Item()
	key := item.Key()

	s, p, o, err := bit.decodeKey(key)
	if err != nil {
		bit.it.Next()
		return ast.Atom{}, false
	}

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

func (bit *BadgerIterator) Close() error {
	if bit.closed {
		return nil
	}
	bit.closed = true
	bit.it.Close()
	bit.txn.Discard()
	return nil
}

func (bit *BadgerIterator) decodeKey(key []byte) (subject, predicate, object string, err error) {
	if len(key) != keys.TripleKeySize {
		return "", "", "", nil
	}

	sID, pID, oID := keys.DecodeTripleKey(key)

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

	return uint64ToString(sID), uint64ToString(pID), uint64ToString(oID), nil
}

func uint64ToString(id uint64) string {
	return "id:" + string(rune(id))
}
