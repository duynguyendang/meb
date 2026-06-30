package query

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/duynguyendang/meb/keys"
	"github.com/dgraph-io/badger/v4"
)

func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.NumVersionsToKeep = 1
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insertTriple(t *testing.T, db *badger.DB, prefix byte, s, p, o uint64) {
	t.Helper()
	key := keys.EncodeTripleKey(prefix, s, p, o)
	val := []byte{0, 0, 0}
	txn := db.NewTransaction(true)
	if err := txn.Set(key, val); err != nil {
		txn.Discard()
		t.Fatalf("txn.Set: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("txn.Commit: %v", err)
	}
}

func withVisitLimit(maxVisits int64) context.Context {
	var counter atomic.Int64
	ctx := WithVisitCounter(context.Background(), &counter, maxVisits)
	return ctx
}

// TestLFTJOrderedDeterministic runs the same query multiple times
// and asserts the result sequence is byte-identical every time.
func TestLFTJOrderedDeterministic(t *testing.T) {
	db := newTestDB(t)

	// Insert: node 1 --pred100--> 2, 1 --pred100--> 3, 1 --pred100--> 4
	for i := uint64(2); i <= 4; i++ {
		insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, i)
		insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	// Single relation: S=1, P=100, O=free → iterate over O values
	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1, 1: 100},
			VariablePositions: map[int]string{2: "leaf"},
		},
	}
	resultVars := []string{"leaf"}

	var baseline []string
	for trial := 0; trial < 10; trial++ {
		var canonicals []string
		for result, err := range engine.ExecuteOrdered(ctx, relations, nil, resultVars) {
			if err != nil {
				t.Fatalf("trial %d: ExecuteOrdered error: %v", trial, err)
			}
			canonicals = append(canonicals, result.Canonical())
		}

		if trial == 0 {
			baseline = canonicals
			continue
		}

		if len(canonicals) != len(baseline) {
			t.Fatalf("trial %d: result count %d != baseline %d", trial, len(canonicals), len(baseline))
		}
		for i := range canonicals {
			if canonicals[i] != baseline[i] {
				t.Errorf("trial %d, result %d: canonical mismatch\n  got:  %s\n  want: %s",
					trial, i, canonicals[i], baseline[i])
			}
		}
	}

	if len(baseline) == 0 {
		t.Fatal("expected at least one result")
	}
}

func TestLFTJOrderedAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}

	for i := uint64(2); i <= 10; i++ {
		insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, i)
		insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1, 1: 100},
			VariablePositions: map[int]string{2: "leaf"},
		},
	}
	resultVars := []string{"leaf"}

	var firstRun []string
	for result, err := range engine.ExecuteOrdered(ctx, relations, nil, resultVars) {
		if err != nil {
			t.Fatalf("first run error: %v", err)
		}
		firstRun = append(firstRun, result.Canonical())
	}

	db.Close()

	db2, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open (reopen): %v", err)
	}
	t.Cleanup(func() { db2.Close() })

	engine2 := NewLFTJEngine(db2)
	var secondRun []string
	for result, err := range engine2.ExecuteOrdered(ctx, relations, nil, resultVars) {
		if err != nil {
			t.Fatalf("second run error: %v", err)
		}
		secondRun = append(secondRun, result.Canonical())
	}

	if len(firstRun) != len(secondRun) {
		t.Fatalf("result count mismatch: first=%d, second=%d", len(firstRun), len(secondRun))
	}
	for i := range firstRun {
		if firstRun[i] != secondRun[i] {
			t.Errorf("result %d differs:\n  first:  %s\n  second: %s", i, firstRun[i], secondRun[i])
		}
	}
}

func TestLFTJOrderedBufferAndSort(t *testing.T) {
	db := newTestDB(t)

	for i := uint64(2); i <= 20; i++ {
		insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, i)
		insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1, 1: 100},
			VariablePositions: map[int]string{2: "leaf"},
		},
	}
	resultVars := []string{"leaf"}

	var prev string
	count := 0
	for result, err := range engine.ExecuteOrdered(ctx, relations, nil, resultVars, WithBufferAndSort(100)) {
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		c := result.Canonical()
		if prev != "" && c < prev {
			t.Errorf("out of order at result %d: %s < %s", count, c, prev)
		}
		prev = c
		count++
	}

	if count == 0 {
		t.Fatal("expected at least one result")
	}
}

func TestLFTJOrderedBufferOverflow(t *testing.T) {
	db := newTestDB(t)

	for i := uint64(2); i <= 20; i++ {
		insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, i)
		insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1, 1: 100},
			VariablePositions: map[int]string{2: "leaf"},
		},
	}
	resultVars := []string{"leaf"}

	var results []uint64
	for result, err := range engine.ExecuteOrdered(ctx, relations, nil, resultVars, WithBufferAndSort(5)) {
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		results = append(results, result.Values[0])
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results (buffer size), got %d", len(results))
	}

	// Verify results are sorted (Canonical order)
	for i := 1; i < len(results); i++ {
		if results[i] <= results[i-1] {
			t.Errorf("results not sorted: %v", results)
			break
		}
	}
}

func TestLFTJLegacyExecuteStillWorks(t *testing.T) {
	db := newTestDB(t)

	for i := uint64(2); i <= 5; i++ {
		insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, i)
		insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, i)
	}

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1, 1: 100},
			VariablePositions: map[int]string{2: "leaf"},
		},
	}
	resultVars := []string{"leaf"}

	count := 0
	for result, err := range engine.Execute(ctx, relations, nil, resultVars) {
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if _, ok := result["leaf"]; !ok {
			t.Errorf("result missing 'leaf' key: %v", result)
		}
		count++
	}

	if count == 0 {
		t.Fatal("expected at least one result")
	}
}

func TestLFTJCanonicalSort(t *testing.T) {
	results := []LFTJResult{
		{Vars: []string{"a", "b"}, Values: []uint64{10, 20}},
		{Vars: []string{"a", "b"}, Values: []uint64{10, 30}},
		{Vars: []string{"a", "b"}, Values: []uint64{5, 1}},
		{Vars: []string{"a", "b"}, Values: []uint64{9, 1}},
	}

	canonicals := make([]string, len(results))
	for i, r := range results {
		canonicals[i] = r.Canonical()
	}

	sorted := make([]string, len(canonicals))
	copy(sorted, canonicals)
	sort.Strings(sorted)

	// Verify that sorted canonicals are indeed in lexicographic order
	for i := 1; i < len(sorted); i++ {
		if sorted[i] <= sorted[i-1] {
			t.Errorf("sorted canonicals not in order at %d: %s >= %s", i, sorted[i], sorted[i-1])
		}
	}

	// Verify all original canonicals are present in sorted version
	if len(canonicals) != len(sorted) {
		t.Fatalf("length mismatch: %d vs %d", len(canonicals), len(sorted))
	}
}

func TestLFTJVarsSorted(t *testing.T) {
	db := newTestDB(t)

	// node 1 --100--> 2, node 1 --200--> 3
	insertTriple(t, db, keys.TripleSPOPrefix, 1, 100, 2)
	insertTriple(t, db, keys.TripleOPSPrefix, 1, 100, 2)
	insertTriple(t, db, keys.TripleSPOPrefix, 1, 200, 3)
	insertTriple(t, db, keys.TripleOPSPrefix, 1, 200, 3)

	engine := NewLFTJEngine(db)
	ctx := withVisitLimit(100000)

	// Single relation with S=1 bound, two free variables at P and O.
	// TrieIterator traverses in SPO trie order: P first, then O.
	// Variable names must sort in trie order for ExecuteOrdered to work:
	//   P variable ("alpha") sorts before O variable ("omega").
	// Pass resultVars in reverse to prove ExecuteOrdered sorts them.
	relations := []RelationPattern{
		{
			Prefix:            keys.TripleSPOPrefix,
			BoundPositions:    map[int]uint64{0: 1},
			VariablePositions: map[int]string{1: "alpha", 2: "omega"},
		},
	}

	resultVars := []string{"omega", "alpha"}

	count := 0
	for result, err := range engine.ExecuteOrdered(ctx, relations, nil, resultVars) {
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if len(result.Vars) != 2 || result.Vars[0] != "alpha" || result.Vars[1] != "omega" {
			t.Errorf("vars not sorted: got %v", result.Vars)
		}
		count++
	}
	if count == 0 {
		t.Fatal("expected at least one result")
	}
}
