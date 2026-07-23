package adapter

import (
	"fmt"
	"testing"

	"codeberg.org/TauCeti/mangle-go/ast"
	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type mockDict struct {
	forward map[string]uint64
	reverse map[uint64]string
}

func newMockDict() *mockDict {
	return &mockDict{
		forward: make(map[string]uint64),
		reverse: make(map[uint64]string),
	}
}

func (d *mockDict) add(s string, id uint64) {
	d.forward[s] = id
	d.reverse[id] = s
}

func (d *mockDict) GetString(id uint64) (string, error) {
	s, ok := d.reverse[id]
	if !ok {
		return "", fmt.Errorf("id %d not found", id)
	}
	return s, nil
}

func (d *mockDict) GetID(s string) (uint64, error) {
	id, ok := d.forward[s]
	if !ok {
		return 0, fmt.Errorf("string %q not found", s)
	}
	return id, nil
}

func (d *mockDict) GetOrCreateID(s string) (uint64, error) {
	if id, ok := d.forward[s]; ok {
		return id, nil
	}
	id := uint64(len(d.forward) + 1)
	d.forward[s] = id
	d.reverse[id] = s
	return id, nil
}

func openTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insertTriple(t *testing.T, db *badger.DB, sID, pID, oID uint64) {
	t.Helper()
	spoKey := keys.EncodeTripleKey(keys.TripleSPOPrefix, sID, pID, oID)
	opsKey := keys.EncodeTripleKey(keys.TripleOPSPrefix, sID, pID, oID)
	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(spoKey, nil); err != nil {
			return err
		}
		return txn.Set(opsKey, nil)
	})
	if err != nil {
		t.Fatalf("failed to insert triple: %v", err)
	}
}

func TestEmptyIterator(t *testing.T) {
	ei := &EmptyIterator{}
	_, ok := ei.Next()
	if ok {
		t.Fatal("EmptyIterator.Next() should return false")
	}
	if err := ei.Close(); err != nil {
		t.Fatalf("EmptyIterator.Close() error: %v", err)
	}
}

func TestSearch_WrongPredicate(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	a := NewMebAdapter(db, dict)

	it := a.Search("not_triples", []any{"a", "b", "c"})
	defer it.Close()
	_, ok := it.Next()
	if ok {
		t.Fatal("expected empty result for wrong predicate")
	}
}

func TestSearch_WrongArgCount(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	a := NewMebAdapter(db, dict)

	it := a.Search("triples", []any{"a", "b"})
	defer it.Close()
	_, ok := it.Next()
	if ok {
		t.Fatal("expected empty result for wrong arg count")
	}
}

func TestSearch_NoBoundArgs(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	a := NewMebAdapter(db, dict)

	it := a.Search("triples", []any{nil, nil, nil})
	defer it.Close()
	_, ok := it.Next()
	if ok {
		t.Fatal("expected empty result for no bound args")
	}
}

func TestSearch_PredicateOnly(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	dict.add("knows", 2)
	a := NewMebAdapter(db, dict)

	it := a.Search("triples", []any{nil, "knows", nil})
	defer it.Close()
	_, ok := it.Next()
	if ok {
		t.Fatal("expected empty result for predicate-only query")
	}
}

func TestSearch_SubjectBound(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	dict.add("alice", 1)
	dict.add("knows", 2)
	dict.add("bob", 3)

	insertTriple(t, db, 1, 2, 3)

	a := NewMebAdapter(db, dict)
	it := a.Search("triples", []any{"alice", nil, nil})
	defer it.Close()

	atom, ok := it.Next()
	if !ok {
		t.Fatal("expected at least one result")
	}
	if len(atom.Args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(atom.Args))
	}
	s := atom.Args[0].(ast.Constant).Symbol
	p := atom.Args[1].(ast.Constant).Symbol
	o := atom.Args[2].(ast.Constant).Symbol
	if s != "alice" || p != "knows" || o != "bob" {
		t.Errorf("unexpected triple: %s %s %s", s, p, o)
	}

	_, ok = it.Next()
	if ok {
		t.Fatal("expected only one result")
	}
}

func TestSearch_SubjectAndPredicateBound(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	dict.add("alice", 1)
	dict.add("knows", 2)
	dict.add("bob", 3)

	insertTriple(t, db, 1, 2, 3)

	a := NewMebAdapter(db, dict)
	it := a.Search("triples", []any{"alice", "knows", nil})
	defer it.Close()

	atom, ok := it.Next()
	if !ok {
		t.Fatal("expected at least one result")
	}
	s := atom.Args[0].(ast.Constant).Symbol
	p := atom.Args[1].(ast.Constant).Symbol
	o := atom.Args[2].(ast.Constant).Symbol
	if s != "alice" || p != "knows" || o != "bob" {
		t.Errorf("unexpected triple: %s %s %s", s, p, o)
	}
}

func TestSearch_ObjectBound(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	dict.add("alice", 1)
	dict.add("knows", 2)
	dict.add("bob", 3)

	insertTriple(t, db, 1, 2, 3)

	a := NewMebAdapter(db, dict)
	it := a.Search("triples", []any{nil, nil, "bob"})
	defer it.Close()

	atom, ok := it.Next()
	if !ok {
		t.Fatal("expected at least one result")
	}
	s := atom.Args[0].(ast.Constant).Symbol
	p := atom.Args[1].(ast.Constant).Symbol
	o := atom.Args[2].(ast.Constant).Symbol
	if s != "alice" || p != "knows" || o != "bob" {
		t.Errorf("unexpected triple: %s %s %s", s, p, o)
	}
}

func TestSearch_SubjectNotInDict(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()
	a := NewMebAdapter(db, dict)

	it := a.Search("triples", []any{"unknown", nil, nil})
	defer it.Close()
	_, ok := it.Next()
	if ok {
		t.Fatal("expected empty result for unknown subject")
	}
}

func TestBadgerIterator_CloseIdempotent(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()

	txn := db.NewTransaction(false)
	it := NewBadgerIterator(txn, keys.EncodeTripleSPOPrefix(1, 0, 0), dict)

	if err := it.Close(); err != nil {
		t.Fatalf("first Close error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Close error: %v", err)
	}
}

func TestBadgerIterator_NextAfterClose(t *testing.T) {
	db := openTestDB(t)
	dict := newMockDict()

	txn := db.NewTransaction(false)
	it := NewBadgerIterator(txn, keys.EncodeTripleSPOPrefix(1, 0, 0), dict)
	it.Close()

	_, ok := it.Next()
	if ok {
		t.Fatal("Next after Close should return false")
	}
}
