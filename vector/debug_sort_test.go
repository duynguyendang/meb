package vector

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// TestDebugSorting debugs the getTopK sorting.
func TestDebugSorting(t *testing.T) {
	rand.Seed(42)

	// Create registry
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, _ := badger.Open(opts)
	defer db.Close()

	reg := NewRegistry(db)

	// Generate test vectors
	vecA := make([]float32, FullDim)
	for i := range vecA {
		vecA[i] = rand.Float32()*2 - 1
	}

	vecB := make([]float32, FullDim)
	copy(vecB, vecA)
	for i := 0; i < 64; i++ {
		vecB[i] += (rand.Float32() - 0.5) * 0.2
	}

	vecC := make([]float32, FullDim)
	for i := range vecC {
		vecC[i] = rand.Float32()*2 - 1
	}

	// Add vectors
	reg.Add(1000, vecA)
	reg.Add(2000, vecB)
	reg.Add(3000, vecC)
	reg.wg.Wait()

	// Search for vecA
	results, _ := reg.Search(vecA, 5)

	fmt.Printf("\n=== Before getTopK (from scanChunkInt8) ===\n")
	for i, r := range results {
		fmt.Printf("  Result %d: ID=%d, Score=%.6f\n", i, r.ID, r.Score)
	}

	// Manually call getTopK to see what it does
	topK := getTopK(results, 3)

	fmt.Printf("\n=== After getTopK ===\n")
	for i, r := range topK {
		fmt.Printf("  Result %d: ID=%d, Score=%.6f\n", i, r.ID, r.Score)
	}

	// Check if sorted correctly
	for i := 0; i < len(topK)-1; i++ {
		if topK[i].Score < topK[i+1].Score {
			fmt.Printf("ERROR: Not sorted! topK[%d].Score (%.6f) < topK[%d].Score (%.6f)\n",
				i, topK[i].Score, i+1, topK[i+1].Score)
		}
	}
}
