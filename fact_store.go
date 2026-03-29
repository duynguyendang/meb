package meb

import (
	"fmt"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"

	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/factstore"
)

func extractTripleFromAtom(atom ast.Atom) (subject, predicate, object string) {
	if atom.Predicate.Symbol == "triples" && len(atom.Args) >= 3 {
		if constTerm, ok := atom.Args[0].(ast.Constant); ok {
			subject = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[1].(ast.Constant); ok {
			predicate = constTerm.Symbol
		}
		if constTerm, ok := atom.Args[2].(ast.Constant); ok {
			object = constTerm.Symbol
		}
	} else {
		predicate = atom.Predicate.Symbol
		if len(atom.Args) >= 2 {
			if constTerm, ok := atom.Args[0].(ast.Constant); ok {
				subject = constTerm.Symbol
			}
			if constTerm, ok := atom.Args[1].(ast.Constant); ok {
				object = constTerm.Symbol
			}
		}
	}

	return
}

func (m *MEBStore) GetFacts(atom ast.Atom, callback func(ast.Atom) error) error {
	s, p, o := extractTripleFromAtom(atom)

	for fact, err := range m.Scan(s, p, o) {
		if err != nil {
			return err
		}

		resultArgs := make([]ast.BaseTerm, 3)
		resultArgs[0] = ast.Constant{Type: ast.StringType, Symbol: fact.Subject}
		resultArgs[1] = ast.Constant{Type: ast.StringType, Symbol: fact.Predicate}

		objectStr, ok := fact.Object.(string)
		if !ok {
			objectStr = fmt.Sprintf("%v", fact.Object)
		}
		resultArgs[2] = ast.Constant{Type: ast.StringType, Symbol: objectStr}

		resultAtom := ast.Atom{
			Predicate: atom.Predicate,
			Args:      resultArgs,
		}

		if err := callback(resultAtom); err != nil {
			return err
		}
	}

	return nil
}

func (m *MEBStore) Add(atom ast.Atom) bool {
	if m.Contains(atom) {
		return false
	}

	subject, predicate, object := extractTripleFromAtom(atom)

	fact := Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    object,
	}

	err := m.AddFactBatch([]Fact{fact})
	if err != nil {
		return false
	}

	return true
}

// Exists performs an efficient key-only existence check without decoding strings.
func (m *MEBStore) Exists(s, p, o string) bool {
	sID, pID, oID, sBound, pBound, oBound, err := m.resolveScanIDs(s, p, o)
	if err != nil {
		return false
	}

	// Pack IDs with current topic for symmetric lookup
	if sBound {
		sID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(sID))
	}
	if oBound {
		oID = keys.PackID(m.topicID.Load(), keys.UnpackLocalID(oID))
	}

	prefix := keys.EncodeTripleSPOPrefix(sID, pID, oID)

	found := false
	err = m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 1
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != keys.TripleKeySize {
				continue
			}
			fs, fp, fo := keys.DecodeTripleKey(key)
			if sBound && fs != sID {
				continue
			}
			if pBound && fp != pID {
				continue
			}
			if oBound && fo != oID {
				continue
			}
			found = true
			return nil
		}
		return nil
	})

	return err == nil && found
}

func (m *MEBStore) Contains(atom ast.Atom) bool {
	s, p, o := extractTripleFromAtom(atom)
	return m.Exists(s, p, o)
}

func (m *MEBStore) ListPredicates() []ast.PredicateSym {
	return []ast.PredicateSym{{Symbol: "triples", Arity: 3}}
}

func (m *MEBStore) Merge(other factstore.ReadOnlyFactStore) error {
	for _, pred := range other.ListPredicates() {
		if err := other.GetFacts(ast.NewQuery(pred), func(atom ast.Atom) error {
			m.Add(atom)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
