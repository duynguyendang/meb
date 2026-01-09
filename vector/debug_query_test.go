package vector

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

// TestDebugQueryBehavior debugs the query test behavior.
func TestDebugQueryBehavior(t *testing.T) {
	rand.Seed(42)

	// Create registry
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	reg := NewRegistry(db)

	// Generate test vectors (same as query test)
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
	idA := uint64(1000)
	idB := uint64(2000)
	idC := uint64(3000)

	err = reg.Add(idA, vecA)
	assert.NoError(t, err)
	err = reg.Add(idB, vecB)
	assert.NoError(t, err)
	err = reg.Add(idC, vecC)
	assert.NoError(t, err)

	// Wait for async operations
	reg.wg.Wait()

	// Check count
	fmt.Printf("Registry count: %d\n", reg.Count())

	// Search for vecA
	results, err := reg.Search(vecA, 5)
	assert.NoError(t, err)

	fmt.Printf("Search results: %d\n", len(results))
	for i, r := range results {
		fmt.Printf("  Result %d: ID=%d, Score=%.6f\n", i, r.ID, r.Score)
	}

	// The top result should be idA (exact match)
	if len(results) > 0 {
		fmt.Printf("Expected top ID: %d, Actual: %d\n", idA, results[0].ID)
	}
}
