package query_test

import (
	"fmt"
	"testing"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"
	"github.com/stretchr/testify/require"
)

// TestDebugVectorMatch tests if exact vector matches work
func TestDebugVectorMatch(t *testing.T) {
	// Create store
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer dbStore.Close()

	// Create a simple test vector with known values
	vecA := make([]float32, 1536)
	for i := range vecA {
		vecA[i] = 0.1
	}

	// Add to registry
	idA := uint64(100)
	err = dbStore.Vectors().Add(idA, vecA)
	require.NoError(t, err)

	// Verify the vector is stored
	count := dbStore.Vectors().Count()
	fmt.Printf("Vector count: %d\n", count)

	// Search for the exact same vector
	results, err := dbStore.Vectors().Search(vecA, 5)
	require.NoError(t, err)

	fmt.Printf("Search results: %d\n", len(results))
	for i, r := range results {
		fmt.Printf("  Result %d: ID=%d, Score=%.4f\n", i, r.ID, r.Score)
	}

	// Expected: idA with score near 1.0
	if len(results) > 0 {
		require.Equal(t, idA, results[0].ID, "First result should be idA")
		require.Greater(t, results[0].Score, float32(0.99), "Score should be near 1.0")
	}
}

// TestDebugMRLProcessing tests if MRL processing works correctly
func TestDebugMRLProcessing(t *testing.T) {
	// Create a simple test vector
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = 0.1
	}

	// Process MRL
	mrl1 := vector.ProcessMRL(vec)
	mrl2 := vector.ProcessMRL(vec)

	// Check if processing is deterministic
	require.Equal(t, len(mrl1), 64, "MRL vector should be 64 dimensions")
	require.Equal(t, len(mrl2), 64, "MRL vector should be 64 dimensions")

	// Check if dot product is 1.0 (exact match)
	score := vector.DotProduct(mrl1, mrl2)
	fmt.Printf("Dot product of identical vectors: %.4f\n", score)
	require.Greater(t, score, float32(0.99), "Dot product should be near 1.0")

	// Check if magnitude is 1.0
	var sumSquares float32
	for _, v := range mrl1 {
		sumSquares += v * v
	}
	fmt.Printf("Magnitude of MRL vector: %.4f\n", float32(sumSquares))
	require.InDelta(t, 1.0, float64(sumSquares), 1e-5, "MRL vector should be normalized")
}
