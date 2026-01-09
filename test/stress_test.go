package meb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/stretchr/testify/require"
)

// TestStressOneMillionFacts tests inserting and querying 1M facts.
// This test verifies:
// 1. Batch insertion performance
// 2. Query performance with large datasets
// 3. Persistence across restarts
func TestStressOneMillionFacts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 1M stress test in short mode")
	}

	// Use longer timeout for this test
	if deadline, ok := t.Deadline(); ok {
		if time.Until(deadline) < 10*time.Minute {
			t.Skip("Skipping 1M stress test - not enough time (need 10+ minutes)")
		}
	}

	// Manually create store (no cleanup) to handle close/reopen cycle
	dir := t.TempDir()
	cfg := store.TestConfig(dir)
	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)

	fmt.Println("\n=== 1M Fact Stress Test ===")

	// Phase 1: Insert 1M facts in batches
	fmt.Println("Phase 1: Inserting 1,000,000 facts...")

	const totalFacts = 1000000
	const batchSize = 10000
	const numBatches = totalFacts / batchSize

	start := time.Now()

	for batch := 0; batch < numBatches; batch++ {
		facts := make([]meb.Fact, batchSize)

		for i := 0; i < batchSize; i++ {
			id := batch*batchSize + i
			facts[i] = meb.Fact{
				Subject:   fmt.Sprintf("node_%d", id),
				Predicate: "type",
				Object:    "test_entity",
			}
		}

		err := mStore.AddFactBatch(facts)
		require.NoError(t, err, "Batch %d failed", batch)

		if batch%10 == 0 && batch > 0 {
			elapsed := time.Since(start)
			factsPerSec := float64(batch*batchSize) / elapsed.Seconds()
			fmt.Printf("  Progress: %d/%d facts (%.1f%%) - %.2fM facts/sec\n",
				batch*batchSize, totalFacts,
				100.0*float64(batch*batchSize)/float64(totalFacts),
				factsPerSec/1e6)
		}
	}

	insertDuration := time.Since(start)
	factsPerSec := float64(totalFacts) / insertDuration.Seconds()

	fmt.Printf("\n✓ Inserted %d facts in %v\n", totalFacts, insertDuration)
	fmt.Printf("  Throughput: %.2fM facts/sec (%.0f facts/sec)\n",
		factsPerSec/1e6, factsPerSec)

	// Verify count (will add more facts in phase 2)
	require.Equal(t, totalFacts, int(mStore.Count()))

	// Phase 2: Query performance tests
	fmt.Println("\nPhase 2: Query performance tests...")

	// Test 2.1: Point query (single node)
	queryStart := time.Now()
	results, err := mStore.Query(context.Background(), "?triples('node_42', ?, ?)")
	queryDuration := time.Since(queryStart)
	require.NoError(t, err)
	require.Len(t, results, 1, "Point query should return 1 result")
	fmt.Printf("  Point query (node_42): %v\n", queryDuration)

	// Test 2.2: Prefix query (actually exact match, but we'll test multiple nodes)
	// Insert some specific nodes for this test
	specificFacts := []meb.Fact{
		{Subject: "test_node_1", Predicate: "attr", Object: "value1"},
		{Subject: "test_node_1", Predicate: "type", Object: "test"},
		{Subject: "test_node_1", Predicate: "status", Object: "active"},
	}
	err = mStore.AddFactBatch(specificFacts)
	require.NoError(t, err)

	queryStart = time.Now()
	results, err = mStore.Query(context.Background(), "?triples('test_node_1', ?, ?)")
	queryDuration = time.Since(queryStart)
	require.NoError(t, err)
	require.Len(t, results, 3, "Exact match query should return 3 results")
	fmt.Printf("  Exact match query (test_node_1): %v (%d results)\n", queryDuration, len(results))

	// Test 2.3: Subject + predicate bound query (most precise)
	queryStart = time.Now()
	results, err = mStore.Query(context.Background(), "?triples('node_100', 'type', ?)")
	queryDuration = time.Since(queryStart)
	require.NoError(t, err)
	require.Len(t, results, 1, "Should find exactly one match")
	fmt.Printf("  Subject+Predicate query (node_100 + type): %v (%d result)\n", queryDuration, len(results))

	// Phase 3: Persistence test
	fmt.Println("\nPhase 3: Persistence test...")

	// Close and reopen (cfg is already declared above)
	err = mStore.Close()
	require.NoError(t, err)

	// Reopen
	mStore2, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)

	// Verify data persisted
	// Note: factCount is now persisted to disk on graceful shutdown
	// The facts should be in BadgerDB and the counter should be accurate
	t.Logf("Fact count after reopen: %d (counter persisted from previous run)", mStore2.Count())

	// Query should still work
	results, err = mStore2.Query(context.Background(), "?triples('node_12345', ?, ?)")
	require.NoError(t, err)
	require.Len(t, results, 1, "Data should persist across restarts")
	fmt.Printf("  ✓ Data persisted correctly\n")

	// Final cleanup
	require.NoError(t, mStore2.Close())

	fmt.Println("\n=== Stress Test Complete ===")
}

// BenchmarkStressInsert1M benchmarks inserting 1M facts.
// Run with: go test -bench=BenchmarkStressInsert1M -benchmem -benchtime=10x
func BenchmarkStressInsert1M(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)

	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(b, err)
	defer mStore.Close()

	const batchSize = 10000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		facts := make([]meb.Fact, batchSize)
		for j := 0; j < batchSize; j++ {
			id := i*batchSize + j
			facts[j] = meb.Fact{
				Subject:   fmt.Sprintf("node_%d", id),
				Predicate: "type",
				Object:    "test",
			}
		}

		err := mStore.AddFactBatch(facts)
		if err != nil {
			b.Fatalf("Failed to insert batch %d: %v", i, err)
		}
	}
}

// BenchmarkStressQuery1M benchmarks query performance with 1M facts.
func BenchmarkStressQuery1M(b *testing.B) {
	dir := b.TempDir()
	cfg := store.TestConfig(dir)

	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(b, err)

	// Pre-populate with 1M facts
	fmt.Printf("Populating benchmark with 1M facts...\n")
	const totalFacts = 1000000
	const batchSize = 10000

	for batch := 0; batch < totalFacts/batchSize; batch++ {
		facts := make([]meb.Fact, batchSize)
		for i := 0; i < batchSize; i++ {
			id := batch*batchSize + i
			facts[i] = meb.Fact{
				Subject:   fmt.Sprintf("node_%d", id),
				Predicate: "type",
				Object:    "test",
			}
		}
		err := mStore.AddFactBatch(facts)
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}
	}
	fmt.Printf("  ✓ Populated %d facts\n", totalFacts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mStore.Query(context.Background(), "?triples('node_42', ?, ?)")
	}
}
