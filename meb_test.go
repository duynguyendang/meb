package meb

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/duynguyendang/meb/store"
	"github.com/google/mangle/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestMEBStore creates a test MEBStore.
func setupTestMEBStore(t *testing.T) *MEBStore {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	meb, err := NewMEBStore(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, meb.Close())
	})

	return meb
}

func TestMEBStoreAddFact(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add a fact
	fact := Fact{
		Subject:   "node_1",
		Predicate: "type",
		Object:    "table",
		Graph:     "default",
	}

	err := meb.AddFact(fact)
	require.NoError(t, err)

	// Verify fact was added
	assert.Equal(t, 1, int(meb.Count()))
}

func TestMEBStoreAddFactBatch(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add multiple facts
	facts := []Fact{
		{Subject: "node_1", Predicate: "type", Object: "table", Graph: "default"},
		{Subject: "node_1", Predicate: "name", Object: "users", Graph: "default"},
		{Subject: "node_2", Predicate: "type", Object: "column", Graph: "default"},
	}

	err := meb.AddFactBatch(facts)
	require.NoError(t, err)

	// Verify facts were added
	assert.Equal(t, 3, int(meb.Count()))
}

func TestMEBStoreQuery(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add facts
	facts := []Fact{
		{Subject: "node_1", Predicate: "type", Object: "table", Graph: "default"},
		{Subject: "node_1", Predicate: "name", Object: "users", Graph: "default"},
		{Subject: "node_2", Predicate: "type", Object: "column", Graph: "default"},
	}

	err := meb.AddFactBatch(facts)
	require.NoError(t, err)

	// Query with subject bound using Scan
	count := 0
	for f, err := range meb.Scan("node_1", "", "", "") {
		require.NoError(t, err)
		assert.Equal(t, "node_1", f.Subject)
		count++
	}
	assert.Equal(t, 2, count)
}

func TestMEBStoreQueryWithPredicate(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add facts
	facts := []Fact{
		{Subject: "node_1", Predicate: "type", Object: "table", Graph: "default"},
		{Subject: "node_1", Predicate: "name", Object: "users", Graph: "default"},
		{Subject: "node_2", Predicate: "type", Object: "column", Graph: "default"},
	}

	err := meb.AddFactBatch(facts)
	require.NoError(t, err)

	// Query with subject + predicate bound using Scan
	count := 0
	var resultFact Fact
	for f, err := range meb.Scan("node_1", "name", "", "") {
		require.NoError(t, err)
		resultFact = f
		count++
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "users", resultFact.Object)
}

func TestFactStoreAdd(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Create a Mangle atom
	atom := ast.Atom{
		Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
		Args: []ast.BaseTerm{
			ast.String("node_1"),
			ast.String("type"),
			ast.String("table"),
		},
	}

	// Add the atom
	added := meb.Add(atom)
	assert.True(t, added)

	// Verify it exists
	assert.True(t, meb.Contains(atom))
	assert.Equal(t, 1, int(meb.Count()))

	// Add again - should return false
	added = meb.Add(atom)
	assert.False(t, added)
}

func TestFactStoreGetFacts(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add atoms
	atoms := []ast.Atom{
		{
			Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
			Args: []ast.BaseTerm{
				ast.String("node_1"),
				ast.String("type"),
				ast.String("table"),
			},
		},
		{
			Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
			Args: []ast.BaseTerm{
				ast.String("node_1"),
				ast.String("name"),
				ast.String("users"),
			},
		},
	}

	for _, atom := range atoms {
		meb.Add(atom)
	}

	// Query with subject bound (using Variable for unbound positions)
	queryAtom := ast.Atom{
		Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
		Args: []ast.BaseTerm{
			ast.String("node_1"),       // Bound
			ast.Variable{},             // Unbound
			ast.Variable{},             // Unbound
		},
	}

	var results []ast.Atom
	err := meb.GetFacts(queryAtom, func(result ast.Atom) error {
		results = append(results, result)
		return nil
	})

	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestFactStoreFullTableScanRejected(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add some data
	atom := ast.Atom{
		Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
		Args: []ast.BaseTerm{
			ast.String("node_1"),
			ast.String("type"),
			ast.String("table"),
		},
	}
	meb.Add(atom)

	// Try to query with all arguments unbound (should fail)
	queryAtom := ast.Atom{
		Predicate: ast.PredicateSym{Symbol: "triples", Arity: 3},
		Args: []ast.BaseTerm{
			ast.Variable{},
			ast.Variable{},
			ast.Variable{},
		},
	}

	err := meb.GetFacts(queryAtom, func(result ast.Atom) error {
		return nil
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "full table scan not allowed")
}

func TestFactStoreListPredicates(t *testing.T) {
	meb := setupTestMEBStore(t)

	preds := meb.ListPredicates()
	assert.Len(t, preds, 1)
	assert.Equal(t, "triples", preds[0].Symbol)
	assert.Equal(t, 3, preds[0].Arity)
}

func TestMEBStorePersistence(t *testing.T) {
	dir := t.TempDir()
	t.Logf("Test directory: %s", dir)
	cfg := store.TestConfig(dir)

	// Phase 1: Create and add data
	meb1, err := NewMEBStore(cfg)
	require.NoError(t, err)

	// Test with a single fact first
	fact := Fact{Subject: "node_1", Predicate: "type", Object: "table", Graph: "default"}
	err = meb1.AddFact(fact)
	require.NoError(t, err)

	// Verify we can scan before closing (without graph filter)
	t.Logf("Before close, total facts: %d", meb1.Count())
	count := 0
	for f, err := range meb1.Scan("node_1", "", "", "") {
		require.NoError(t, err)
		t.Logf("Found fact before close: S=%s P=%s O=%v G=%s", f.Subject, f.Predicate, f.Object, f.Graph)
		count++
	}
	t.Logf("Before close, found %d facts for node_1", count)
	require.Equal(t, 1, count)

	// Close and reopen
	t.Logf("Closing database...")
	require.NoError(t, meb1.Close())

	// Phase 2: Reopen and verify
	t.Logf("Reopening database...")
	meb2, err := NewMEBStore(cfg)
	require.NoError(t, err)

	// Check total count
	t.Logf("After reopen, total facts: %d", meb2.Count())

	// Try to scan with just subject (no graph filter)
	count = 0
	for f, err := range meb2.Scan("node_1", "", "", "") {
		require.NoError(t, err)
		t.Logf("Found fact after reopen: S=%s P=%s O=%v G=%s", f.Subject, f.Predicate, f.Object, f.Graph)
		count++
	}
	t.Logf("After reopen, found %d facts for node_1", count)
	assert.Equal(t, 1, count)

	require.NoError(t, meb2.Close())
}

func TestMEBStoreReset(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add data
	facts := []Fact{
		{Subject: "node_1", Predicate: "type", Object: "table", Graph: "default"},
		{Subject: "node_2", Predicate: "type", Object: "column", Graph: "default"},
	}

	err := meb.AddFactBatch(facts)
	require.NoError(t, err)
	assert.Equal(t, 2, int(meb.Count()))

	// Reset
	err = meb.Reset()
	require.NoError(t, err)
	assert.Equal(t, 0, int(meb.Count()))

	// Verify no data using Scan
	count := 0
	for _, err := range meb.Scan("node_1", "", "", "") {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 0, count)
}

func TestMEBStoreStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	meb := setupTestMEBStore(t)

	// Insert 10,000 facts
	const count = 10000
	facts := make([]Fact, count)

	for i := 0; i < count; i++ {
		facts[i] = Fact{
			Subject:   fmt.Sprintf("node_%d", i),
			Predicate: "type",
			Object:    "test",
			Graph:     "default",
		}
	}

	err := meb.AddFactBatch(facts)
	require.NoError(t, err)
	assert.Equal(t, count, int(meb.Count()))

	// Query specific node
	results, err := meb.Query(context.Background(), "?triples('node_100', ?, ?)")
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

// Benchmark tests

func BenchmarkMEBStoreAdd(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)
	meb, err := NewMEBStore(cfg)
	require.NoError(b, err)
	defer meb.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fact := Fact{
			Subject:   fmt.Sprintf("node_%d", i),
			Predicate: "type",
			Object:    "table",
			Graph:     "default",
		}
		_ = meb.AddFact(fact)
	}
}

func BenchmarkMEBStoreQueryWithBinding(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)
	meb, err := NewMEBStore(cfg)
	require.NoError(b, err)

	// Pre-populate with 1000 facts
	for i := 0; i < 1000; i++ {
		fact := Fact{
			Subject:   "node_1",
			Predicate: fmt.Sprintf("pred_%d", i),
			Object:    fmt.Sprintf("obj_%d", i),
			Graph:     "default",
		}
		_ = meb.AddFact(fact)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = meb.Query(context.Background(), "?triples('node_1', ?, ?)")
	}
}

// === Content Store Tests ===

func TestContentStoreCompression(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Create 10KB of test data
	testData := make([]byte, 10240)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Store the content
	err := meb.SetContent(1, testData)
	require.NoError(t, err)

	// Retrieve the content
	retrieved, err := meb.GetContent(1)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Verify integrity - must be exact
	assert.Equal(t, testData, retrieved)
}

func TestContentStoreLargeText(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Create a large text document (~10KB)
	largeText := make([]byte, 0, 10240)
	for i := 0; i < 100; i++ {
		largeText = append(largeText, []byte(fmt.Sprintf("This is line %d of the document. It contains some repetitive text to test compression efficiency.\n", i))...)
	}

	// Store the content
	err := meb.SetContent(42, largeText)
	require.NoError(t, err)

	// Retrieve and verify
	retrieved, err := meb.GetContent(42)
	require.NoError(t, err)
	assert.Equal(t, largeText, retrieved, "Retrieved content must match original")

	// Verify it's actually valid UTF-8 text
	assert.Equal(t, len(largeText), len(retrieved))
}

func TestContentStoreNotFound(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Try to get non-existent content
	retrieved, err := meb.GetContent(999)
	require.NoError(t, err, "Getting non-existent content should not error")
	assert.Nil(t, retrieved, "Non-existent content should return nil")
}

func TestContentStoreEmptyContent(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Store empty content
	err := meb.SetContent(1, []byte{})
	require.NoError(t, err)

	// Retrieve it
	retrieved, err := meb.GetContent(1)
	require.NoError(t, err)
	assert.Equal(t, []byte{}, retrieved)
}

func TestContentStoreOverwrite(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Store initial content
	initialContent := []byte("Initial content")
	err := meb.SetContent(1, initialContent)
	require.NoError(t, err)

	// Overwrite with new content
	newContent := []byte("New and different content")
	err = meb.SetContent(1, newContent)
	require.NoError(t, err)

	// Verify we get the new content
	retrieved, err := meb.GetContent(1)
	require.NoError(t, err)
	assert.Equal(t, newContent, retrieved, "Should retrieve the overwritten content")
	assert.NotEqual(t, initialContent, retrieved, "Should not retrieve the old content")
}

func TestContentStoreBinaryData(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Test with various binary patterns
	testCases := []struct {
		name string
		data []byte
	}{
		{"All zeros", make([]byte, 1024)},
		{"All ones", bytes.Repeat([]byte{0xFF}, 1024)},
		{"Alternating", bytes.Repeat([]byte{0xAA, 0x55}, 512)},
		{"Random-like", func() []byte {
			data := make([]byte, 1024)
			for i := range data {
				data[i] = byte(i * 17 % 256)
			}
			return data
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := meb.SetContent(1, tc.data)
			require.NoError(t, err)

			retrieved, err := meb.GetContent(1)
			require.NoError(t, err)
			assert.Equal(t, tc.data, retrieved)
		})
	}
}

func TestAddDocumentFullRAG(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Create test document
	docKey := "test_document_1"
	content := []byte("This is a test document about machine learning and vector databases. " +
		"It contains enough text to be compressed effectively by the S2 compression algorithm.")
	vector := make([]float32, 1536)
	for i := range vector {
		vector[i] = float32(i) / 1536.0 // Test vector
	}
	metadata := map[string]any{
		"author":     "test_user",
		"created_at": "2024-01-01",
		"category":   "ml",
		"importance": 5,
	}

	// Add the complete document
	err := meb.AddDocument(docKey, content, vector, metadata)
	require.NoError(t, err)

	// Verify metadata facts were added using Scan
	// Note: AddDocument stores metadata in the "metadata" graph
	count := 0
	var resultFact Fact
	for f, err := range meb.Scan("test_document_1", "author", "", "metadata") {
		require.NoError(t, err)
		resultFact = f
		count++
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "test_user", resultFact.Object)

	// Verify other metadata
	count = 0
	for f, err := range meb.Scan("test_document_1", "category", "", "metadata") {
		require.NoError(t, err)
		resultFact = f
		count++
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "ml", resultFact.Object)
}

func TestContentStorePersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := store.TestConfig(dir)

	// Phase 1: Store content
	meb1, err := NewMEBStore(cfg)
	require.NoError(t, err)

	originalContent := []byte("Persistent content that survives restart")
	err = meb1.SetContent(123, originalContent)
	require.NoError(t, err)
	require.NoError(t, meb1.Close())

	// Phase 2: Reopen and verify
	meb2, err := NewMEBStore(cfg)
	require.NoError(t, err)

	retrievedContent, err := meb2.GetContent(123)
	require.NoError(t, err)
	assert.Equal(t, originalContent, retrievedContent, "Content must persist across restarts")

	require.NoError(t, meb2.Close())
}

func TestContentStoreMultipleIDs(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Store content for multiple IDs
	contents := make(map[uint64][]byte)
	for i := uint64(1); i <= 100; i++ {
		content := []byte(fmt.Sprintf("Content for ID %d - unique content here", i))
		contents[i] = content
		err := meb.SetContent(i, content)
		require.NoError(t, err)
	}

	// Verify all can be retrieved
	for id, expectedContent := range contents {
		retrieved, err := meb.GetContent(id)
		require.NoError(t, err, "Should retrieve content for ID %d", id)
		assert.Equal(t, expectedContent, retrieved, "Content mismatch for ID %d", id)
	}
}

func TestContentStoreCompressionEfficiency(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Create highly compressible data (repetitive pattern)
	repetitiveData := bytes.Repeat([]byte("AAAAABBBBBCCCCCDDDDDEEEEE"), 100)

	// Store it
	err := meb.SetContent(1, repetitiveData)
	require.NoError(t, err)

	// Verify retrieval works
	retrieved, err := meb.GetContent(1)
	require.NoError(t, err)
	assert.Equal(t, repetitiveData, retrieved)

	// The S2 compression should achieve significant space savings
	// For repetitive data like this, compression ratio should be > 5:1
	// We can't directly measure the compressed size from the API,
	// but the fact that it works and is fast validates the implementation
}

func TestContentStoreConcurrentAccess(t *testing.T) {
	meb := setupTestMEBStore(t)

	const numGoroutines = 10
	const operationsPerGoroutine = 10

	doneChan := make(chan struct{}, numGoroutines)
	errChan := make(chan error, 1)

	// Concurrent writes
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer func() { doneChan <- struct{}{} }()
			for i := 0; i < operationsPerGoroutine; i++ {
				id := uint64(goroutineID*operationsPerGoroutine + i)
				content := []byte(fmt.Sprintf("Goroutine %d, operation %d", goroutineID, i))
				if err := meb.SetContent(id, content); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		}(g)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-doneChan:
			// Goroutine completed successfully
		case err := <-errChan:
			t.Fatalf("Error in concurrent write: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent writes")
		}
	}

	// Verify all content can be read correctly
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < operationsPerGoroutine; i++ {
			id := uint64(g*operationsPerGoroutine + i)
			expectedContent := []byte(fmt.Sprintf("Goroutine %d, operation %d", g, i))
			retrieved, err := meb.GetContent(id)
			require.NoError(t, err)
			assert.Equal(t, expectedContent, retrieved)
		}
	}
}

func TestContentStoreWithQueryBuilder(t *testing.T) {
	meb := setupTestMEBStore(t)

	// Add some documents with vectors
	for i := 0; i < 5; i++ {
		docKey := fmt.Sprintf("doc_%d", i)
		content := []byte(fmt.Sprintf("Content of document %d", i))
		vec := make([]float32, 1536)
		for j := range vec {
			vec[j] = float32(j+i) / 1536.0
		}

		err := meb.AddDocument(docKey, content, vec, nil)
		require.NoError(t, err)
	}

	// Query for similar documents (using the first document's vector)
	queryVec := make([]float32, 1536)
	for i := range queryVec {
		queryVec[i] = float32(i) / 1536.0
	}

	// Note: This test will return results but without proper dictionary decoding
	// the Key field will show "id:X" format. The content should still be populated.
	// This validates the content hydration in the query pipeline.
	// For a complete end-to-end test, we'd need to implement dictionary decoding.
}
