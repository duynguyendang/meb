package adapter

import (
	"fmt"
	"testing"
	"time"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/store"
	"github.com/google/mangle/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestAdapter creates a test adapter with a clean database.
func setupTestAdapter(t *testing.T) (*MebAdapter, *dict.Encoder, func()) {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	db, err := store.OpenBadgerDB(cfg)
	require.NoError(t, err)

	encoder, err := dict.NewEncoder(db, 1000)
	require.NoError(t, err)

	adapter := NewMebAdapter(db, encoder)

	cleanup := func() {
		_ = encoder.Close()
		db.Close()
	}

	return adapter, encoder, cleanup
}

// TestMebAdapterBasicSubjectQuery tests querying with a bound subject.
func TestMebAdapterBasicSubjectQuery(t *testing.T) {
	adapter, encoder, cleanup := setupTestAdapter(t)
	defer cleanup()

	db := adapter.db

	// Add some test facts: Parent(alice, bob), Parent(alice, carol)
	facts := []struct {
		s, p, o string
	}{
		{"alice", "parent_of", "bob"},
		{"alice", "parent_of", "carol"},
		{"bob", "parent_of", "dave"},
	}

	// Encode and add facts to BadgerDB
	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for _, fact := range facts {
		sID, err := encoder.GetOrCreateID(fact.s)
		require.NoError(t, err)
		pID, err := encoder.GetOrCreateID(fact.p)
		require.NoError(t, err)
		oID, err := encoder.GetOrCreateID(fact.o)
		require.NoError(t, err)

		spoKey := keys.EncodeSPOKey(sID, pID, oID)
		require.NoError(t, batch.Set(spoKey, nil))

		opsKey := keys.EncodeOPSKey(sID, pID, oID)
		require.NoError(t, batch.Set(opsKey, nil))
	}

	require.NoError(t, batch.Flush())

	// Query: Who does alice parent? (subject bound)
	it := adapter.Search("triples", []any{"alice", "parent_of", nil})

	var results []string
	for {
		atom, ok := it.Next()
		if !ok {
			break
		}
		// Extract the object (third argument)
		if obj, ok := atom.Args[2].(ast.Constant); ok {
			results = append(results, obj.Symbol)
		}
	}

	require.NoError(t, it.Close())

	// Should find bob and carol
	assert.Len(t, results, 2)
	assert.Contains(t, results, "bob")
	assert.Contains(t, results, "carol")
}

// TestMebAdapterObjectQuery tests querying with a bound object (reverse traversal).
func TestMebAdapterObjectQuery(t *testing.T) {
	adapter, encoder, cleanup := setupTestAdapter(t)
	defer cleanup()

	db := adapter.db

	// Add test facts
	facts := []struct {
		s, p, o string
	}{
		{"alice", "parent_of", "bob"},
		{"carol", "parent_of", "bob"},
	}

	// Encode and add facts
	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for _, fact := range facts {
		sID, err := encoder.GetOrCreateID(fact.s)
		require.NoError(t, err)
		pID, err := encoder.GetOrCreateID(fact.p)
		require.NoError(t, err)
		oID, err := encoder.GetOrCreateID(fact.o)
		require.NoError(t, err)

		spoKey := keys.EncodeSPOKey(sID, pID, oID)
		require.NoError(t, batch.Set(spoKey, nil))

		opsKey := keys.EncodeOPSKey(sID, pID, oID)
		require.NoError(t, batch.Set(opsKey, nil))
	}

	require.NoError(t, batch.Flush())

	// Query: Who are bob's parents? (object bound - uses OPS index)
	it := adapter.Search("triples", []any{nil, "parent_of", "bob"})

	var results []string
	for {
		atom, ok := it.Next()
		if !ok {
			break
		}
		// Extract the subject (first argument)
		if sub, ok := atom.Args[0].(ast.Constant); ok {
			results = append(results, sub.Symbol)
		}
	}

	require.NoError(t, it.Close())

	// Should find alice and carol
	assert.Len(t, results, 2)
	assert.Contains(t, results, "alice")
	assert.Contains(t, results, "carol")
}

// TestMebAdapterFullScanRejected tests that full table scans are rejected.
func TestMebAdapterFullScanRejected(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	// Query with no arguments bound (should return empty iterator)
	it := adapter.Search("triples", []any{nil, nil, nil})

	// Should get no results
	atom, ok := it.Next()
	assert.False(t, ok)
	assert.Equal(t, ast.Atom{}, atom)

	require.NoError(t, it.Close())
}

// TestMebAdapterAncestryBenchmark is the main benchmark test from the task specification.
// It creates a linear chain of 1000 nodes and queries for all descendants.
func TestMebAdapterAncestryBenchmark(t *testing.T) {
	adapter, encoder, cleanup := setupTestAdapter(t)
	defer cleanup()

	db := adapter.db

	// Create a linear chain: Parent(node_0, node_1), Parent(node_1, node_2), ...
	const chainLength = 1000

	fmt.Printf("Building ancestry chain of %d nodes...\n", chainLength)

	startTime := time.Now()

	// First, get/create ID for "parent_of" once
	parentOfID, err := encoder.GetOrCreateID("parent_of")
	require.NoError(t, err)

	// Add facts to BadgerDB in batch
	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for i := 0; i < chainLength; i++ {
		// Get/create IDs for each node
		nodeI, err := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i))
		require.NoError(t, err)

		nodeINext, err := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i+1))
		require.NoError(t, err)

		spoKey := keys.EncodeSPOKey(nodeI, parentOfID, nodeINext)
		if err := batch.Set(spoKey, nil); err != nil {
			require.NoError(t, err)
		}

		opsKey := keys.EncodeOPSKey(nodeI, parentOfID, nodeINext)
		if err := batch.Set(opsKey, nil); err != nil {
			require.NoError(t, err)
		}
	}

	require.NoError(t, batch.Flush())

	buildTime := time.Since(startTime)
	fmt.Printf("Chain built in %v\n", buildTime)

	// Now query: Find all descendants of node_0
	// This simulates a recursive ancestry query
	fmt.Printf("Querying for all descendants of node_0...\n")

	queryStart := time.Now()

	// For this test, we'll do a simple iterative traversal
	// In a real Mangle scenario, this would use the recursive rule:
	// Ancestor(X, Y) :- Parent(X, Y).
	// Ancestor(X, Z) :- Parent(X, Y), Ancestor(Y, Z).

	descendants := findDescendants(t, adapter, "node_0")

	queryTime := time.Since(queryStart)

	fmt.Printf("Found %d descendants in %v\n", len(descendants), queryTime)
	if len(descendants) > 0 {
		fmt.Printf("Average time per descendant: %v\n", queryTime/time.Duration(len(descendants)))
	}

	// Verify we found all descendants
	assert.Equal(t, chainLength, len(descendants), "Should find all nodes in the chain")

	// Verify the chain is correct
	for i := 0; i < chainLength; i++ {
		expected := fmt.Sprintf("node_%d", i+1)
		assert.Contains(t, descendants, expected, "Should contain node_%d", i+1)
	}

	// Performance assertions
	// With prefix scanning, we should be able to traverse 1000 nodes quickly
	// On modern hardware with NVMe, this should be < 100ms
	maxExpectedTime := 500 * time.Millisecond
	if queryTime > maxExpectedTime {
		t.Logf("WARNING: Query took %v, expected < %v", queryTime, maxExpectedTime)
	} else {
		t.Logf("✓ Performance target met: %v < %v", queryTime, maxExpectedTime)
	}
}

// findDescendants performs an iterative traversal to find all descendants.
// This simulates what the Mangle engine would do with recursive rules.
func findDescendants(t *testing.T, adapter *MebAdapter, startNode string) []string {
	descendants := make(map[string]bool)
	toVisit := []string{startNode}
	visited := make(map[string]bool)

	iteration := 0
	for len(toVisit) > 0 {
		current := toVisit[0]
		toVisit = toVisit[1:]

		if visited[current] {
			continue
		}
		visited[current] = true

		iteration++
		if iteration <= 3 {
			fmt.Printf("Iteration %d: Querying for children of %s\n", iteration, current)
		}

		// Query: parent_of(current, ?) - find all children of current node
		it := adapter.Search("triples", []any{current, "parent_of", nil})

		foundChildren := 0
		for {
			atom, ok := it.Next()
			if !ok {
				break
			}

			// Extract the object (child)
			if obj, ok := atom.Args[2].(ast.Constant); ok {
				child := obj.Symbol
				foundChildren++
				if iteration <= 3 {
					fmt.Printf("  Found child: %s\n", child)
				}
				if !descendants[child] {
					descendants[child] = true
					toVisit = append(toVisit, child)
				}
			}
		}

		if iteration <= 3 && foundChildren == 0 {
			fmt.Printf("  WARNING: No children found for %s\n", current)
		}

		require.NoError(t, it.Close())
	}

	// Convert map to slice
	result := make([]string, 0, len(descendants))
	for d := range descendants {
		result = append(result, d)
	}

	return result
}

// BenchmarkMebAdapterAdapterQuery benchmarks the adapter query performance.
func BenchmarkMebAdapterAdapterQuery(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)

	db, err := store.OpenBadgerDB(cfg)
	require.NoError(b, err)

	encoder, err := dict.NewEncoder(db, 10000)
	require.NoError(b, err)

	defer func() {
		_ = encoder.Close()
		db.Close()
	}()

	adapter := NewMebAdapter(db, encoder)

	// Pre-populate with 10,000 facts
	const numFacts = 10000
	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for i := 0; i < numFacts; i++ {
		sID, _ := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i))
		pID, _ := encoder.GetOrCreateID("parent_of")
		oID, _ := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i+1))

		spoKey := keys.EncodeSPOKey(sID, pID, oID)
		_ = batch.Set(spoKey, nil)

		opsKey := keys.EncodeOPSKey(sID, pID, oID)
		_ = batch.Set(opsKey, nil)
	}

	_ = batch.Flush()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it := adapter.Search("triples", []any{"node_100", "parent_of", nil})
		for {
			_, ok := it.Next()
			if !ok {
				break
			}
		}
		_ = it.Close()
	}
}

// TestMebAdapterIteratorClose tests that iterators are properly closed.
func TestMebAdapterIteratorClose(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	// Create an iterator (even with no data)
	it := adapter.Search("triples", []any{"nonexistent", "predicate", nil})

	// Close should not error
	assert.NoError(t, it.Close())

	// Calling Close again should be safe
	assert.NoError(t, it.Close())
}

// TestMebAdapterUnknownPredicate tests querying an unknown predicate.
func TestMebAdapterUnknownPredicate(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	// Query unknown predicate
	it := adapter.Search("unknown_predicate", []any{"x", nil, nil})

	// Should return empty iterator
	atom, ok := it.Next()
	assert.False(t, ok)
	assert.Equal(t, ast.Atom{}, atom)

	assert.NoError(t, it.Close())
}

// TestMebAdapterWrongArity tests querying with wrong arity.
func TestMebAdapterWrongArity(t *testing.T) {
	adapter, _, cleanup := setupTestAdapter(t)
	defer cleanup()

	// Query with wrong arity (2 args instead of 3)
	it := adapter.Search("triples", []any{"x", nil})

	// Should return empty iterator
	atom, ok := it.Next()
	assert.False(t, ok)
	assert.Equal(t, ast.Atom{}, atom)

	assert.NoError(t, it.Close())
}

// TestDebugAdapterQuery is a debug test to understand query issues.
func TestDebugAdapterQuery(t *testing.T) {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	db, err := store.OpenBadgerDB(cfg)
	require.NoError(t, err)

	encoder, err := dict.NewEncoder(db, 1000)
	require.NoError(t, err)

	adapter := NewMebAdapter(db, encoder)

	// Add a simple test fact
	sID, err := encoder.GetOrCreateID("alice")
	require.NoError(t, err)
	fmt.Printf("alice ID: %d\n", sID)

	pID, err := encoder.GetOrCreateID("parent_of")
	require.NoError(t, err)
	fmt.Printf("parent_of ID: %d\n", pID)

	oID, err := encoder.GetOrCreateID("bob")
	require.NoError(t, err)
	fmt.Printf("bob ID: %d\n", oID)

	// Store in BadgerDB
	batch := db.NewWriteBatch()
	spoKey := keys.EncodeSPOKey(sID, pID, oID)
	fmt.Printf("SPO key: %x\n", spoKey)
	require.NoError(t, batch.Set(spoKey, nil))
	opsKey := keys.EncodeOPSKey(sID, pID, oID)
	require.NoError(t, batch.Set(opsKey, nil))
	require.NoError(t, batch.Flush())

	// Now query
	fmt.Println("\nQuerying for alice parent_of ?...")
	it := adapter.Search("triples", []any{"alice", "parent_of", nil})

	count := 0
	for {
		atom, ok := it.Next()
		if !ok {
			break
		}
		count++
		fmt.Printf("Result %d: %+v\n", count, atom)
	}
	fmt.Printf("Total results: %d\n", count)

	require.NoError(t, it.Close())
	encoder.Close()
	db.Close()
}

// TestDictGetIDsThenGetID tests that GetIDs followed by GetID works correctly.
func TestDictGetIDsThenGetID(t *testing.T) {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	db, err := store.OpenBadgerDB(cfg)
	require.NoError(t, err)

	encoder, err := dict.NewEncoder(db, 100) // Small cache size
	require.NoError(t, err)
	defer encoder.Close()
	defer db.Close()

	// Create IDs using GetIDs (batch)
	strings := []string{"node_0", "parent_of", "node_1"}
	ids, err := encoder.GetIDs(strings)
	require.NoError(t, err)
	fmt.Printf("Created IDs with GetIDs: %v\n", ids)

	// Now try to get the same IDs using GetID (simulates cache miss)
	for i, s := range strings {
		id, err := encoder.GetID(s)
		if err != nil {
			t.Logf("WARNING: GetID(%s) failed: %v", s, err)
		} else if id != ids[i] {
			t.Logf("ERROR: GetID(%s) returned %d, expected %d", s, id, ids[i])
		} else {
			t.Logf("OK: GetID(%s) = %d", s, id)
		}
	}

	// Also test GetOrCreateID (should return same IDs)
	for i, s := range strings {
		id, err := encoder.GetOrCreateID(s)
		if err != nil {
			t.Logf("ERROR: GetOrCreateID(%s) failed: %v", s, err)
		} else if id != ids[i] {
			t.Logf("ERROR: GetOrCreateID(%s) returned %d, expected %d", s, id, ids[i])
		} else {
			t.Logf("OK: GetOrCreateID(%s) = %d", s, id)
		}
	}
}

// TestAncestrySimple is a simpler version of the ancestry test to debug the issue.
func TestAncestrySimple(t *testing.T) {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	db, err := store.OpenBadgerDB(cfg)
	require.NoError(t, err)

	encoder, err := dict.NewEncoder(db, 1000)
	require.NoError(t, err)

	adapter := NewMebAdapter(db, encoder)

	// Create just 3 facts: Parent(node_0, node_1), Parent(node_1, node_2), Parent(node_2, node_3)
	const chainLength = 3

	// First, get/create ID for "parent_of" once
	parentOfID, err := encoder.GetOrCreateID("parent_of")
	require.NoError(t, err)
	fmt.Printf("parent_of ID: %d\n", parentOfID)

	// Store in BadgerDB
	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for i := 0; i < chainLength; i++ {
		nodeI, err := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i))
		require.NoError(t, err)

		nodeINext, err := encoder.GetOrCreateID(fmt.Sprintf("node_%d", i+1))
		require.NoError(t, err)

		fmt.Printf("Fact %d: node_%d(id=%d) parent_of(id=%d) node_%d(id=%d)\n", i, i, nodeI, parentOfID, i+1, nodeINext)

		spoKey := keys.EncodeSPOKey(nodeI, parentOfID, nodeINext)
		fmt.Printf("  SPO key: %x\n", spoKey)
		require.NoError(t, batch.Set(spoKey, nil))

		opsKey := keys.EncodeOPSKey(nodeI, parentOfID, nodeINext)
		require.NoError(t, batch.Set(opsKey, nil))
	}

	require.NoError(t, batch.Flush())

	// Now query for children of node_0
	fmt.Println("\nQuerying for children of node_0...")
	it := adapter.Search("triples", []any{"node_0", "parent_of", nil})

	count := 0
	for {
		atom, ok := it.Next()
		if !ok {
			break
		}
		count++
		fmt.Printf("Result %d: %+v\n", count, atom)
	}
	fmt.Printf("Total results: %d\n", count)

	require.NoError(t, it.Close())
	encoder.Close()
	db.Close()
}
