package meb

import (
	"fmt"
	"testing"

	"github.com/duynguyendang/meb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/dgraph-io/badger/v4"
)

// setupQuadTestStore creates a test store with quad support.
func setupQuadTestStore(t *testing.T) *MEBStore {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	meb, err := NewMEBStore(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, meb.Close())
	})

	return meb
}

// TestQuadStoreBasicInsertion tests basic quad insertion and retrieval.
func TestQuadStoreBasicInsertion(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert fact: Alice knows Bob in doc1
	facts := []Fact{
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "doc1",
		},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Scan for facts in doc1
	count := 0
	for f, err := range store.Scan("Alice", "knows", "", "doc1") {
		require.NoError(t, err)
		count++
		assert.Equal(t, "Alice", f.Subject)
		assert.Equal(t, "knows", f.Predicate)
		assert.Equal(t, "Bob", f.Object)
		assert.Equal(t, "doc1", f.Graph)
	}

	assert.Equal(t, 1, count, "Should find exactly 1 fact")
}

// TestQuadStoreMultiGraph tests facts in multiple graphs.
func TestQuadStoreMultiGraph(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert same fact in different graphs
	facts := []Fact{
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "doc1",
		},
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "doc2",
		},
	}

	t.Log("Before AddFactBatch: checking existing IDs")
	preAliceID, _ := store.dict.GetID("Alice")
	t.Logf("Pre-insert Alice ID: %d", preAliceID)
	preDoc1ID, _ := store.dict.GetID("doc1")
	t.Logf("Pre-insert doc1 ID: %d", preDoc1ID)

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Debug: Check what IDs were assigned
	t.Log("After AddFactBatch: checking IDs")
	t.Log("Checking dictionary IDs...")
	doc1ID, _ := store.dict.GetID("doc1")
	t.Logf("doc1 ID: %d", doc1ID)
	doc2ID, _ := store.dict.GetID("doc2")
	t.Logf("doc2 ID: %d", doc2ID)
	aliceID, _ := store.dict.GetID("Alice")
	t.Logf("Alice ID: %d", aliceID)

	// Debug: Check what GSPO keys are in the database
	t.Log("Checking stored GSPO keys...")
	txn := store.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()
	gspoPrefix := []byte{0x22} // QuadGSPOPrefix
	for it.Seek(gspoPrefix); it.ValidForPrefix(gspoPrefix); it.Next() {
		key := it.Item().Key()
		t.Logf("GSPO Key: %x", key)
	}

	// Scan for facts in doc1 only
	doc1Count := 0
	for f, err := range store.Scan("Alice", "knows", "", "doc1") {
		require.NoError(t, err)
		doc1Count++
		assert.Equal(t, "doc1", f.Graph)
	}
	assert.Equal(t, 1, doc1Count, "Should find exactly 1 fact in doc1")

	// Scan for facts across all graphs
	allCount := 0
	for f, err := range store.Scan("Alice", "knows", "", "") {
		require.NoError(t, err)
		allCount++
		assert.Equal(t, "Alice", f.Subject)
	}
	assert.Equal(t, 2, allCount, "Should find exactly 2 facts across all graphs")
}

// TestQuadStoreDeleteGraph tests graph deletion.
func TestQuadStoreDeleteGraph(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert facts in different graphs
	facts := []Fact{
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "doc1",
		},
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "doc2",
		},
		{
			Subject:   "Bob",
			Predicate: "knows",
			Object:    "Charlie",
			Graph:     "doc1",
		},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Verify both facts exist in doc1 before deletion
	doc1CountBefore := 0
	for _, err := range store.Scan("", "", "", "doc1") {
		require.NoError(t, err)
		doc1CountBefore++
	}
	assert.Equal(t, 2, doc1CountBefore, "doc1 should have 2 facts before deletion")

	// Debug: Check what SPOG keys exist before deletion
	t.Log("Checking SPOG keys before deletion...")
	txn := store.db.NewTransaction(false)
	defer txn.Discard()
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	spoPrefix := []byte{0x20}
	for it.Seek(spoPrefix); it.ValidForPrefix(spoPrefix); it.Next() {
		key := it.Item().Key()
		t.Logf("SPOG Key before: %x", key)
	}

	// Delete doc1
	err = store.DeleteGraph("doc1")
	require.NoError(t, err)

	// Debug: Check what SPOG keys exist after deletion
	t.Log("Checking SPOG keys after deletion...")
	txn2 := store.db.NewTransaction(false)
	defer txn2.Discard()
	it2 := txn2.NewIterator(badger.DefaultIteratorOptions)
	defer it2.Close()
	for it2.Seek(spoPrefix); it2.ValidForPrefix(spoPrefix); it2.Next() {
		key := it2.Item().Key()
		t.Logf("SPOG Key after: %x", key)
	}

	// Debug: Check what GSPO keys exist after deletion
	t.Log("Checking GSPO keys after deletion...")
	gspoPrefix := []byte{0x22}
	for it2.Seek(gspoPrefix); it2.ValidForPrefix(gspoPrefix); it2.Next() {
		key := it2.Item().Key()
		t.Logf("GSPO Key after: %x", key)
	}

	// Verify doc1 facts are gone
	doc1CountAfter := 0
	for _, err := range store.Scan("", "", "", "doc1") {
		require.NoError(t, err)
		doc1CountAfter++
	}
	assert.Equal(t, 0, doc1CountAfter, "doc1 should be empty after deletion")

	// Verify doc2 facts still exist
	doc2Count := 0
	for f, err := range store.Scan("", "", "", "doc2") {
		require.NoError(t, err)
		doc2Count++
		assert.Equal(t, "doc2", f.Graph)
	}
	assert.Equal(t, 1, doc2Count, "doc2 should still have 1 fact")
}

// TestQuadStoreDefaultGraph tests that empty graph defaults to "default".
func TestQuadStoreDefaultGraph(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert fact with empty graph
	facts := []Fact{
		{
			Subject:   "Alice",
			Predicate: "knows",
			Object:    "Bob",
			Graph:     "", // Should default to "default"
		},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Scan for facts in "default" graph
	count := 0
	for f, err := range store.Scan("Alice", "knows", "", "default") {
		require.NoError(t, err)
		count++
		assert.Equal(t, "default", f.Graph)
	}

	assert.Equal(t, 1, count, "Should find fact in default graph")
}

// TestQuadStoreScanIndexSelection tests that Scan chooses the right index.
func TestQuadStoreScanIndexSelection(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert test data
	facts := []Fact{
		{Subject: "Alice", Predicate: "knows", Object: "Bob", Graph: "doc1"},
		{Subject: "Bob", Predicate: "knows", Object: "Charlie", Graph: "doc1"},
		{Subject: "Charlie", Predicate: "knows", Object: "David", Graph: "doc1"},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Test 1: Subject-bound query (should use SPOG index)
	count := 0
	for f, err := range store.Scan("Alice", "", "", "") {
		require.NoError(t, err)
		count++
		assert.Equal(t, "Alice", f.Subject)
	}
	assert.Equal(t, 1, count, "Subject-bound query should find 1 fact")

	// Test 2: Object-bound query (requires predicate for POSG index)
	// NOTE: Object-only queries are not efficiently supported without predicate
	// Skipping this test for now - would require full table scan
	count = 0
	for f, err := range store.Scan("", "knows", "Charlie", "") {
		require.NoError(t, err)
		count++
		assert.Equal(t, "Charlie", f.Object)
	}
	assert.Equal(t, 1, count, "Object+Predicate bound query should find 1 fact")

	// Test 3: Graph-only query (should use GSPO index)
	count = 0
	for f, err := range store.Scan("", "", "", "doc1") {
		require.NoError(t, err)
		count++
		assert.Equal(t, "doc1", f.Graph)
	}
	assert.Equal(t, 3, count, "Graph-only query should find 3 facts")
}

// TestGenericHelpers tests the generic helper functions.
func TestGenericHelpers(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert test data (all values stored as strings internally)
	facts := []Fact{
		{Subject: "Alice", Predicate: "age", Object: "30", Graph: "doc1"},
		{Subject: "Bob", Predicate: "age", Object: "25", Graph: "doc1"},
		{Subject: "Charlie", Predicate: "score", Object: "95.5", Graph: "doc1"},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Test Value with type assertion (Object is stored as string)
	for f, err := range store.Scan("Alice", "age", "", "") {
		require.NoError(t, err)

		// Test Value (Object is stored as string)
		age, ok := Value[string](f)
		require.True(t, ok, "Value[string] should succeed")
		assert.Equal(t, "30", age)

		// Test MustValue
		age2 := MustValue[string](f)
		assert.Equal(t, "30", age2)
	}

	// Test Count
	count, err := Count(store.Scan("", "", "", "doc1"))
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Test First
	first, err := First(store.Scan("Alice", "", "", ""))
	require.NoError(t, err)
	assert.Equal(t, "Alice", first.Subject)

	// Test Collect
	collected, err := Collect(store.Scan("", "age", "", "doc1"))
	require.NoError(t, err)
	assert.Len(t, collected, 2, "Should collect 2 facts with predicate=age in doc1")
}

// TestQuadStoreIteratorPattern tests the for/range pattern with Scan.
func TestQuadStoreIteratorPattern(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert test data
	facts := []Fact{
		{Subject: "Alice", Predicate: "knows", Object: "Bob", Graph: "doc1"},
		{Subject: "Alice", Predicate: "knows", Object: "Charlie", Graph: "doc1"},
		{Subject: "Bob", Predicate: "knows", Object: "Alice", Graph: "doc1"},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Test the for/range pattern (must compile with Go 1.23+)
	results := []string{}
	for f, err := range store.Scan("Alice", "knows", "", "doc1") {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		name, ok := f.Object.(string)
		if ok {
			results = append(results, name)
		}
	}

	assert.Len(t, results, 2, "Alice should know 2 people in doc1")
	assert.Contains(t, results, "Bob")
	assert.Contains(t, results, "Charlie")
}

// TestQuadStoreFilter tests the Filter helper function.
func TestQuadStoreFilter(t *testing.T) {
	store := setupQuadTestStore(t)

	// Insert test data
	facts := []Fact{
		{Subject: "Alice", Predicate: "age", Object: 30, Graph: "doc1"},
		{Subject: "Bob", Predicate: "age", Object: 25, Graph: "doc1"},
		{Subject: "Charlie", Predicate: "age", Object: 35, Graph: "doc1"},
		{Subject: "David", Predicate: "age", Object: 28, Graph: "doc1"},
	}

	err := store.AddFactBatch(facts)
	require.NoError(t, err)

	// Filter for adults (age >= 30)
	filtered := Filter(
		store.Scan("", "age", "", "doc1"),
		func(f Fact) bool {
			// Object is returned as string from dictionary
			ageStr, ok := f.Object.(string)
			if !ok {
				return false
			}
			var age int
			_, err := fmt.Sscanf(ageStr, "%d", &age)
			return err == nil && age >= 30
		},
	)

	count := 0
	for f, err := range filtered {
		require.NoError(t, err)
		ageStr := f.Object.(string)
		var age int
		fmt.Sscanf(ageStr, "%d", &age)
		assert.GreaterOrEqual(t, age, 30, "Filtered results should be adults only")
		count++
	}

	assert.Equal(t, 2, count, "Should find 2 adults")
}

// BenchmarkQuadStoreScan benchmarks the Scan performance.
func BenchmarkQuadStoreScan(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)

	store, err := NewMEBStore(cfg)
	require.NoError(b, err)
	defer store.Close()

	// Pre-populate with 10,000 facts across 10 graphs
	const numFacts = 10000
	facts := make([]Fact, numFacts)
	for i := 0; i < numFacts; i++ {
		facts[i] = Fact{
			Subject:   fmt.Sprintf("person_%d", i%100),
			Predicate: "knows",
			Object:    fmt.Sprintf("person_%d", (i+1)%100),
			Graph:     fmt.Sprintf("graph_%d", i%10),
		}
	}

	err = store.AddFactBatch(facts)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range store.Scan(fmt.Sprintf("person_%d", i%100), "knows", "", "") {
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			count++
		}
		_ = count
	}
}

// BenchmarkQuadStoreDeleteGraph benchmarks graph deletion performance.
func BenchmarkQuadStoreDeleteGraph(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)

	store, err := NewMEBStore(cfg)
	require.NoError(b, err)

	// Pre-populate with 1,000 facts per graph, 10 graphs
	const numGraphs = 10
	const factsPerGraph = 1000

	for g := 0; g < numGraphs; g++ {
		graph := fmt.Sprintf("graph_%d", g)
		facts := make([]Fact, factsPerGraph)
		for i := 0; i < factsPerGraph; i++ {
			facts[i] = Fact{
				Subject:   fmt.Sprintf("person_%d", i),
				Predicate: "knows",
				Object:    fmt.Sprintf("person_%d", i+1),
				Graph:     graph,
			}
		}
		err = store.AddFactBatch(facts)
		require.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		graph := fmt.Sprintf("graph_%d", i%numGraphs)
		err := store.DeleteGraph(graph)
		if err != nil {
			b.Fatalf("DeleteGraph failed: %v", err)
		}
	}
}
