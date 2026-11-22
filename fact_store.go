package meb

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"

	"codeberg.org/TauCeti/mangle-go/ast"
	"codeberg.org/TauCeti/mangle-go/factstore"
)

var triplesPredSym = ast.PredicateSym{Symbol: "triples", Arity: 3}

func constantToTripleString(c ast.Constant) (string, bool) {
	switch c.Type {
	case ast.StringType, ast.NameType, ast.BytesType:
		return c.Symbol, true
	default:
		return "", false
	}
}

func constantToFactObject(c ast.Constant) any {
	switch c.Type {
	case ast.StringType, ast.NameType, ast.BytesType:
		return c.Symbol
	case ast.NumberType:
		if c.NumValue >= math.MinInt32 && c.NumValue <= math.MaxInt32 {
			return int32(c.NumValue)
		}
		return fmt.Sprintf("%d", c.NumValue)
	case ast.Float64Type:
		f := math.Float64frombits(uint64(c.NumValue))
		return float32(f)
	case ast.TimeType, ast.DurationType:
		return fmt.Sprintf("%d", c.NumValue)
	default:
		return c.String()
	}
}

func factObjectToConstant(obj any) ast.Constant {
	switch v := obj.(type) {
	case string:
		return ast.String(v)
	case bool:
		if v {
			return ast.Number(1)
		}
		return ast.Number(0)
	case int32:
		return ast.Number(int64(v))
	case int64:
		return ast.Number(v)
	case int:
		return ast.Number(int64(v))
	case float32:
		return ast.Float64(float64(v))
	case float64:
		return ast.Float64(v)
	default:
		return ast.String(fmt.Sprintf("%v", obj))
	}
}

func extractTripleFromAtom(atom ast.Atom) (subject, predicate, object string) {
	if atom.Predicate.Symbol == "triples" && len(atom.Args) >= 3 {
		if constTerm, ok := atom.Args[0].(ast.Constant); ok {
			if s, isStr := constantToTripleString(constTerm); isStr {
				subject = s
			}
		}
		if constTerm, ok := atom.Args[1].(ast.Constant); ok {
			if p, isStr := constantToTripleString(constTerm); isStr {
				predicate = p
			}
		}
		if constTerm, ok := atom.Args[2].(ast.Constant); ok {
			if o, isStr := constantToTripleString(constTerm); isStr {
				object = o
			}
		}
	} else {
		predicate = atom.Predicate.Symbol
		if len(atom.Args) >= 2 {
			if constTerm, ok := atom.Args[0].(ast.Constant); ok {
				if s, isStr := constantToTripleString(constTerm); isStr {
					subject = s
				}
			}
			if constTerm, ok := atom.Args[1].(ast.Constant); ok {
				if o, isStr := constantToTripleString(constTerm); isStr {
					object = o
				}
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

		resultArgs := []ast.BaseTerm{
			ast.String(fact.Subject),
			ast.String(fact.Predicate),
			factObjectToConstant(fact.Object),
		}

		if len(atom.Args) == 3 && !factstore.Matches(atom.Args, resultArgs) {
			continue
		}

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
	subject, predicate, object := extractTripleFromAtom(atom)
	var objVal any = object
	if atom.Predicate.Symbol == "triples" && len(atom.Args) >= 3 {
		if constTerm, ok := atom.Args[2].(ast.Constant); ok {
			objVal = constantToFactObject(constTerm)
		}
	}

	fact := Fact{
		Subject:   subject,
		Predicate: predicate,
		Object:    objVal,
	}

	// Use a single read-write transaction so Contains() + AddFactBatch are atomic
	// with respect to other writers. BadgerDB has no unique-key constraint, so
	// without this transaction two concurrent Add() calls with the same atom
	// could both observe "not present" and produce duplicates.
	added := false
	err := m.Update(func(t *StoreTxn) error {
		if t.Exists(subject, predicate, object) {
			return nil
		}
		added = true
		return t.AddFact(fact)
	})
	if err != nil {
		return false
	}
	return added
}

// EstimateFactCount returns the estimated number of facts in the store.
func (m *MEBStore) EstimateFactCount() int {
	return int(m.numFacts.Load())
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
	if atom.Predicate == triplesPredSym && len(atom.Args) == 3 {
		if _, ok := atom.Args[2].(ast.Constant); ok {
			found := false
			_ = m.GetFacts(atom, func(ast.Atom) error {
				found = true
				return nil
			})
			return found
		}
	}
	s, p, o := extractTripleFromAtom(atom)
	return m.Exists(s, p, o)
}

func (m *MEBStore) ListPredicates() []ast.PredicateSym {
	return []ast.PredicateSym{triplesPredSym}
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

// MergeBatch imports facts from another store in batches, avoiding the O(N²)
// per-fact Exists() check. Existing facts are silently skipped.
func (m *MEBStore) MergeBatch(other factstore.ReadOnlyFactStore, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	for _, pred := range other.ListPredicates() {
		batch := make([]Fact, 0, batchSize)

		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := m.AddFactBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}

		err := other.GetFacts(ast.NewQuery(pred), func(atom ast.Atom) error {
			s, p, o := extractTripleFromAtom(atom)
			batch = append(batch, Fact{Subject: s, Predicate: p, Object: o})
			if len(batch) >= batchSize {
				return flush()
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}

// MangleFactStore adapts a topic-scoped MEBStore into Mangle's
// factstore.FactStoreWithRemove interface. Triple facts (triples/3) stream
// directly from MEBStore for the configured topicID, while derived IDB
// predicates and non-triple EDB predicates live in an in-memory overlay backed
// by Mangle v0.7's collision-safe SimpleInMemoryStore.
type MangleFactStore struct {
	store   *MEBStore
	topicID uint32
	overlay factstore.SimpleInMemoryStore
	mu      sync.RWMutex
}

var _ factstore.FactStoreWithRemove = (*MangleFactStore)(nil)

// FactStore returns a Mangle FactStoreWithRemove scoped to the store's current TopicID.
func (m *MEBStore) FactStore() *MangleFactStore {
	return m.FactStoreForTopic(m.TopicID())
}

// FactStoreForTopic returns a Mangle FactStoreWithRemove scoped to topicID.
func (m *MEBStore) FactStoreForTopic(topicID uint32) *MangleFactStore {
	if topicID == 0 {
		topicID = m.TopicID()
	}
	return &MangleFactStore{
		store:   m,
		topicID: topicID,
		overlay: factstore.NewSimpleInMemoryStore(),
	}
}

func (fs *MangleFactStore) GetFacts(query ast.Atom, fn func(ast.Atom) error) error {
	if query.Predicate == triplesPredSym {
		s, p, o := extractTripleFromAtom(query)
		for fact, err := range fs.store.ScanInTopicContext(context.Background(), fs.topicID, s, p, o) {
			if err != nil {
				return err
			}
			resultArgs := []ast.BaseTerm{
				ast.String(fact.Subject),
				ast.String(fact.Predicate),
				factObjectToConstant(fact.Object),
			}
			if !factstore.Matches(query.Args, resultArgs) {
				continue
			}
			if err := fn(ast.Atom{Predicate: triplesPredSym, Args: resultArgs}); err != nil {
				return err
			}
		}
		return nil
	}

	fs.mu.RLock()
	var matched []ast.Atom
	err := fs.overlay.GetFacts(query, func(a ast.Atom) error {
		matched = append(matched, a)
		return nil
	})
	fs.mu.RUnlock()
	if err != nil {
		return err
	}
	for _, a := range matched {
		if err := fn(a); err != nil {
			return err
		}
	}
	return nil
}

func (fs *MangleFactStore) Contains(atom ast.Atom) bool {
	if atom.Predicate == triplesPredSym {
		found := false
		_ = fs.GetFacts(atom, func(ast.Atom) error {
			found = true
			return errStopScan
		})
		return found
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.overlay.Contains(atom)
}

var errStopScan = fmt.Errorf("stop")

func (fs *MangleFactStore) ListPredicates() []ast.PredicateSym {
	fs.mu.RLock()
	overlayPreds := fs.overlay.ListPredicates()
	fs.mu.RUnlock()

	preds := make([]ast.PredicateSym, 0, len(overlayPreds)+1)
	preds = append(preds, triplesPredSym)
	for _, p := range overlayPreds {
		if p != triplesPredSym {
			preds = append(preds, p)
		}
	}
	return preds
}

func (fs *MangleFactStore) EstimateFactCount() int {
	fs.mu.RLock()
	n := fs.overlay.EstimateFactCount()
	fs.mu.RUnlock()
	return fs.store.EstimateFactCount() + n
}

func (fs *MangleFactStore) Add(atom ast.Atom) bool {
	if atom.Predicate == triplesPredSym && len(atom.Args) == 3 {
		s, p, _ := extractTripleFromAtom(atom)
		var obj any
		if c, ok := atom.Args[2].(ast.Constant); ok {
			obj = constantToFactObject(c)
		}
		if s == "" || p == "" || obj == nil {
			return false
		}
		if fs.Contains(atom) {
			return false
		}
		added := false
		err := fs.store.Update(func(txn *StoreTxn) error {
			added = true
			return txn.AddFactBatchWithTopic([]Fact{{Subject: s, Predicate: p, Object: obj}}, fs.topicID)
		})
		return err == nil && added
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.overlay.Add(atom)
}

func (fs *MangleFactStore) Remove(atom ast.Atom) bool {
	if atom.Predicate == triplesPredSym && len(atom.Args) == 3 {
		s, p, _ := extractTripleFromAtom(atom)
		var obj any
		if c, ok := atom.Args[2].(ast.Constant); ok {
			obj = constantToFactObject(c)
		}
		if s == "" || p == "" || obj == nil {
			return false
		}
		deleted, err := fs.store.DeleteFactWithTopic(Fact{Subject: s, Predicate: p, Object: obj}, fs.topicID)
		return err == nil && deleted
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.overlay.Remove(atom)
}

func (fs *MangleFactStore) Merge(other factstore.ReadOnlyFactStore) {
	for _, pred := range other.ListPredicates() {
		_ = other.GetFacts(ast.NewQuery(pred), func(atom ast.Atom) error {
			fs.Add(atom)
			return nil
		})
	}
}
