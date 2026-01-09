package meb_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/stretchr/testify/require"
)

// TestStress10MillionFacts tests inserting and querying 10M facts with local buffering.
// This test verifies the performance improvements from:
// 1. Sharded dictionary (16-way)
// 2. Local buffering in workers (5000 facts per batch)
// 3. 10 concurrent workers
//
// RUN: go test -v ./test/ -run TestStress10MillionFacts -timeout 10m
func TestStress10MillionFacts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 10M stress test in short mode")
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("   10 MILLION FACT STRESS TEST (10 Workers, Local Buffering)")
	fmt.Println(strings.Repeat("=", 70))

	// Use production-like configuration with sharded dictionary
	dataDir := t.TempDir()
	cfg := store.DefaultConfig(dataDir)
	cfg.NumDictShards = 16 // Enable sharded dictionary for parallel writes

	fmt.Printf("\nConfiguration:\n")
	fmt.Printf("  Data Directory: %s\n", dataDir)
	fmt.Printf("  Block Cache: 8.0 GiB\n")
	fmt.Printf("  Index Cache: 1.0 GiB\n")
	fmt.Printf("  LRU Cache: 100000 items\n")
	fmt.Printf("  Compression: true\n")
	fmt.Printf("  Dictionary: 16-way sharded (for parallel writes)\n")
	fmt.Printf("  Workers: 10 (concurrent insertion)\n")
	fmt.Printf("  Worker Buffer: 5000 facts\n")

	mStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer mStore.Close()

	// Phase 1: Insert 10M facts with 10 workers using local buffering
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("PHASE 1: INSERTION (10,000,000 facts)")
	fmt.Println(strings.Repeat("-", 70))

	const totalFacts = 10_000_000
	const numWorkers = 10
	const workerBufferSize = 5000 // Each worker buffers 5000 facts before flushing
	const progressInterval = 1_000_000 // Print progress every 1M facts

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

	// Test 2.1: Point queries (100 random entities)
	fmt.Println("\nTest 2.1: Point Queries (100 entities)")
	pointStart := time.Now()
	for i := 0; i < 100; i++ {
		entityID := (i * 99773) % totalFacts
		results, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
		require.Len(t, results, 1)
	}
	pointDuration := time.Since(pointStart)
	fmt.Printf("  100 point queries: %v (%.2f ms per query)\n",
		pointDuration, float64(pointDuration.Milliseconds())/100)

	// Test 2.2: Batch query performance (1000 queries)
	fmt.Println("\nTest 2.2: Batch Query Workload (1000 queries)")
	batchStart := time.Now()
	for i := 0; i < 1000; i++ {
		entityID := (i * 99991) % totalFacts
		_, err := mStore.Query(context.Background(),
			fmt.Sprintf("?triples('entity_%d', ?, ?)", entityID))
		require.NoError(t, err)
	}
	batchDuration := time.Since(batchStart)
	fmt.Printf("  1000 queries: %v (%.2f ms per query, %.0f queries/sec)\n",
		batchDuration,
		float64(batchDuration.Milliseconds())/1000,
		1000.0/batchDuration.Seconds())

	// Summary
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("✓ 10M STRESS TEST PASSED")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Total Test Duration: %v\n", time.Since(start))
	fmt.Printf("\nPerformance Summary:\n")
	fmt.Printf("  Insert throughput: %.2fM facts/sec\n", totalThroughput/1e6)
	fmt.Printf("  Point query latency: %.2f ms\n", float64(pointDuration.Milliseconds())/100)
	fmt.Printf("  Batch query throughput: %.0f queries/sec\n", 1000.0/batchDuration.Seconds())
}
