package meb_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/stretchr/testify/require"
)

// TestStress100MillionFacts tests inserting and querying 100M facts.
//
// This is an EXTREMELY heavy test that:
// - Requires ~30-50GB of disk space
// - Takes 10-20 minutes with 10 concurrent workers
// - Tests production-scale performance
//
// RUN MANUALLY: STRESS_TEST_100M=true go test -v -run TestStress100MillionFacts -timeout 30m
func TestStress100MillionFacts(t *testing.T) {
	// Require explicit opt-in via environment variable
	if os.Getenv("STRESS_TEST_100M") != "true" {
		t.Skip("Skipping 100M stress test - set STRESS_TEST_100M=true to enable")
	}

	// Always skip in short mode
	if testing.Short() {
		t.Skip("Skipping 100M stress test in short mode")
	}

	// Check for sufficient timeout (10 workers should finish in ~20 min)
	if deadline, ok := t.Deadline(); ok {
		remaining := time.Until(deadline)
		t.Logf("Deadline: %v, remaining: %v", deadline, remaining)
		if remaining < 20*time.Minute {
			t.Skip("Skipping 100M stress test - need at least 20 minutes")
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("   100 MILLION FACT STRESS TEST (10 Workers)")
	fmt.Println("   ⚠️  This test will take ~15-20 minutes and require 30-50GB disk space")
	fmt.Println(strings.Repeat("=", 70))

	// Use production-like configuration
	dataDir := os.TempDir() + "/meb-stress-100m-" + fmt.Sprint(time.Now().Unix())
	os.MkdirAll(dataDir, 0755)
	defer os.RemoveAll(dataDir)

	cfg := store.DefaultConfig(dataDir)
	cfg.NumDictShards = 16 // Enable sharded dictionary for parallel writes

	fmt.Printf("\nConfiguration:\n")
	fmt.Printf("  Data Directory: %s\n", dataDir)
	fmt.Printf("  Block Cache: %s\n", formatBytes(cfg.BlockCacheSize))
	fmt.Printf("  Index Cache: %s\n", formatBytes(cfg.IndexCacheSize))
	fmt.Printf("  LRU Cache: %d items\n", cfg.LRUCacheSize)
	fmt.Printf("  Compression: %v\n", cfg.Compression)
	fmt.Printf("  Dictionary: 16-way sharded (for parallel writes)\n")
	fmt.Printf("  Workers: 10 (concurrent insertion)\n")

	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer mStore.Close()

	// Phase 1: Insert 100M facts in batches with 10 concurrent workers
	// Workers use local buffering (5000 facts) before calling AddFactBatch
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("PHASE 1: INSERTION (100,000,000 facts with 10 workers)")
	fmt.Println(strings.Repeat("-", 70))

	const totalFacts = 100_000_000
	const numWorkers = 10
	const workerBufferSize = 5000 // Each worker buffers 5000 facts before flushing
	const progressInterval = 10_000_000 // Print progress every 10M facts

	start := time.Now()

	// Channel to distribute individual facts
	factChan := make(chan meb.Fact, numWorkers*100)
	// Channel to collect errors
	errChan := make(chan error, 1)
	// Atomic counter for completed facts
	var completedFacts atomic.Int64

	// Progress reporter goroutine
	var wg sync.WaitGroup
	progressDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				completed := completedFacts.Load()
				if completed > 0 && int(completed)%progressInterval == 0 {
					now := time.Now()
					totalElapsed := now.Sub(start)
					totalThroughput := float64(completed) / totalElapsed.Seconds()

					fmt.Printf("[%s] %7d/%7d facts (%6.2f%%) | Overall: %8.0f facts/s\n",
						formatDuration(totalElapsed),
						completed,
						totalFacts,
						100.0*float64(completed)/float64(totalFacts),
						totalThroughput)

					if completed >= totalFacts {
						return
					}
				}
			case <-progressDone:
				return
			}
		}
	}()

	// Start workers with local buffering
	for workerID := 0; workerID < numWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Local buffer (like a truck collecting goods)
			buffer := make([]meb.Fact, 0, workerBufferSize)

			for fact := range factChan {
				buffer = append(buffer, fact)

				// When buffer is full, deliver to database
				if len(buffer) >= workerBufferSize {
					if err := mStore.AddFactBatch(buffer); err != nil {
						select {
						case errChan <- fmt.Errorf("worker %d: %w", id, err):
						default:
						}
						return
					}
					completedFacts.Add(int64(len(buffer)))
					// Reset buffer for next batch
					buffer = buffer[:0]
				}
			}

			// Flush remaining facts in buffer
			if len(buffer) > 0 {
				if err := mStore.AddFactBatch(buffer); err != nil {
					select {
					case errChan <- fmt.Errorf("worker %d (final flush): %w", id, err):
					default:
					}
					return
				}
				completedFacts.Add(int64(len(buffer)))
			}
		}(workerID)
	}

	// Feed facts to workers
	fmt.Println("Generating and distributing facts...")
	for i := 0; i < totalFacts; i++ {
		entityType := (i % 10) + 1
		propType := (i % 20) + 1

		fact := meb.Fact{
			Subject:   fmt.Sprintf("entity_%d", i),
			Predicate: fmt.Sprintf("property_%d", propType),
			Object:    fmt.Sprintf("value_%d", entityType*1000+propType),
		}

		factChan <- fact
	}
	close(factChan)

	// Wait for all workers to complete
	wg.Wait()
	close(errChan)
	close(progressDone)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	insertDuration := time.Since(start)
	totalThroughput := float64(totalFacts) / insertDuration.Seconds()

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("✓ INSERTION COMPLETE")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Total Facts:      %d\n", totalFacts)
	fmt.Printf("Total Duration:   %v\n", formatDuration(insertDuration))
	fmt.Printf("Throughput:       %.2fM facts/sec (%.0f facts/sec)\n",
		totalThroughput/1e6, totalThroughput)
	fmt.Printf("Avg Latency:      %.2f ms per fact\n",
		(float64(insertDuration.Nanoseconds())/float64(totalFacts))/1e6)

	// Phase 2: Query performance tests
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("PHASE 2: QUERY PERFORMANCE TESTS")
	fmt.Println(strings.Repeat("-", 70))

	// Test 2.1: Point queries (single entity)
	fmt.Println("\nTest 2.1: Point Queries (1000 random entities)")
	pointStart := time.Now()
	for i := 0; i < 1000; i++ {
		entityID := (i * 99773) % totalFacts // Use prime number for distribution
		results, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
		require.Len(t, results, 1)
	}
	pointDuration := time.Since(pointStart)
	fmt.Printf("  1000 point queries: %v (%.2f ms per query)\n",
		pointDuration, float64(pointDuration.Milliseconds())/1000)

	// Test 2.2: Single entity with multiple properties
	fmt.Println("\nTest 2.2: Multi-Property Queries (100 entities)")
	multiStart := time.Now()
	for i := 0; i < 100; i++ {
		entityID := i * 1000003 // Another prime for distribution
		results, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1, "Should have at least 1 property")
	}
	multiDuration := time.Since(multiStart)
	fmt.Printf("  100 multi-property queries: %v (%.2f ms per query)\n",
		multiDuration, float64(multiDuration.Milliseconds())/1000)

	// Test 2.3: Specific property queries
	fmt.Println("\nTest 2.3: Property-Specific Queries (1000 queries)")
	propStart := time.Now()
	for i := 0; i < 1000; i++ {
		entityID := (i * 77717) % totalFacts
		propID := (i % 20) + 1
		results, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', 'property_%d', ?)", entityID, propID))
		require.NoError(t, err)
		require.Len(t, results, 1)
	}
	propDuration := time.Since(propStart)
	fmt.Printf("  1000 property-specific queries: %v (%.2f ms per query)\n",
		propDuration, float64(propDuration.Milliseconds())/1000)

	// Test 2.4: Batch query performance (simulating real workloads)
	fmt.Println("\nTest 2.4: Batch Query Workload (10000 queries)")
	batchStart := time.Now()
	for i := 0; i < 10000; i++ {
		entityID := (i * 99991) % totalFacts
		_, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
	}
	batchDuration := time.Since(batchStart)
	fmt.Printf("  10000 queries: %v (%.2f ms per query, %.0f queries/sec)\n",
		batchDuration,
		float64(batchDuration.Milliseconds())/10000,
		10000.0/batchDuration.Seconds())

	// Phase 3: Persistence verification
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("PHASE 3: PERSISTENCE VERIFICATION")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Println("Closing database...")
	require.NoError(t, mStore.Close())

	fmt.Println("Reopening database...")
	mStore2, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer mStore2.Close()

	fmt.Println("Verifying data integrity...")
	// Query some entities to verify persistence
	for i := 0; i < 100; i++ {
		entityID := i * 1000003
		results, err := mStore2.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1, "Entity %d should exist", entityID)
	}
	fmt.Println("  ✓ Data integrity verified (100 random entities)")

	// Summary
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("✓ 100M STRESS TEST PASSED")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Total Test Duration: %v\n", time.Since(start))
	fmt.Println("\nDatabase statistics:")
	fmt.Printf("  Total facts: %d\n", totalFacts)
	fmt.Printf("  Insert throughput: %.2fM facts/sec\n", totalThroughput/1e6)
	fmt.Printf("  Point query latency: %.2f ms\n", float64(pointDuration.Milliseconds())/1000)
	fmt.Printf("  Batch query throughput: %.0f queries/sec\n", 10000.0/batchDuration.Seconds())
}

// BenchmarkStress100MInsert benchmarks insertion at 100M scale.
// Run with: STRESS_TEST_100M=true go test -bench=BenchmarkStress100MInsert -benchmem -benchtime=1x
func BenchmarkStress100MInsert(b *testing.B) {
	if os.Getenv("STRESS_TEST_100M") != "true" {
		b.Skip("Set STRESS_TEST_100M=true to enable")
	}

	dataDir := os.TempDir() + "/meb-bench-100m-" + fmt.Sprint(time.Now().Unix())
	cfg := store.DefaultConfig(dataDir)

	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(b, err)
	defer os.RemoveAll(dataDir)
	defer mStore.Close()

	const batchSize = 50_000

	fmt.Printf("\nRunning 100M insertion benchmark...\n")
	start := time.Now()

	for i := 0; i < b.N; i++ {
		facts := make([]meb.Fact, batchSize)
		for j := 0; j < batchSize; j++ {
			id := i*batchSize + j
			facts[j] = meb.Fact{
				Subject:   fmt.Sprintf("entity_%d", id),
				Predicate: fmt.Sprintf("property_%d", j%20),
				Object:    fmt.Sprintf("value_%d", id),
			}
		}

		err := mStore.AddFactBatch(facts)
		if err != nil {
			b.Fatalf("Failed to insert batch %d: %v", i, err)
		}

		if i%100 == 0 && i > 0 {
			elapsed := time.Since(start)
			throughput := float64(i*batchSize) / elapsed.Seconds()
			fmt.Printf("  Progress: %d batches (%.2fM facts/s)\n", i, throughput/1e6)
		}
	}
}

// Helper functions

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return d.String()
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, minutes)
}
