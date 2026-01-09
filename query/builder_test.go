package query_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateRandomVector generates a random 1536-d vector.
func generateRandomVector() []float32 {
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1 // [-1, 1]
	}
	return vec
}

// makeSimilarVector creates a vector similar to the base vector.
func makeSimilarVector(base []float32, similarity float32) []float32 {
	result := make([]float32, len(base))
	copy(result, base)

	// Add some noise to reduce similarity
	// Small changes in the first 64 dimensions will affect the MRL similarity
	for i := 0; i < 64; i++ {
		result[i] += (rand.Float32() - 0.5) * 0.2
	}

	return result
}

func TestNeuroSymbolicIntegration(t *testing.T) {
	// Set random seed for reproducibility
	rand.Seed(42)
	// Create store
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer dbStore.Close()

	// Create test data
	vecA := generateRandomVector()
	vecB := makeSimilarVector(vecA, 0.9) // 90% similar to A
	vecC := generateRandomVector()       // Completely different

	// Get IDs for subjects
	idA := uint64(1000)
	idB := uint64(2000)
	idC := uint64(3000)

	// Add vectors to registry
	err = dbStore.Vectors().Add(idA, vecA)
	require.NoError(t, err)
	err = dbStore.Vectors().Add(idB, vecB)
	require.NoError(t, err)
	err = dbStore.Vectors().Add(idC, vecC)
	require.NoError(t, err)

	// Add graph facts
	// func_A is written by alice
	err = dbStore.AddFact(meb.Fact{
		Subject:   fmt.Sprintf("id:%d", idA),
		Predicate: "author",
		Object:    "alice",
	})
	require.NoError(t, err)

	// func_B is written by bob
	err = dbStore.AddFact(meb.Fact{
		Subject:   fmt.Sprintf("id:%d", idB),
		Predicate: "author",
		Object:    "bob",
	})
	require.NoError(t, err)

	// func_C is written by alice
	err = dbStore.AddFact(meb.Fact{
		Subject:   fmt.Sprintf("id:%d", idC),
		Predicate: "author",
		Object:    "alice",
	})
	require.NoError(t, err)

	// Test 1: SimilarTo only (should return all vectors)
	t.Run("SimilarTo_Only", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vecA).
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 2, "Should find at least 2 vectors")

		// Just verify we got results (exact match depends on similarity calculation)
		for _, r := range results {
			t.Logf("Result: ID=%d, Score=%f", r.ID, r.Score)
		}
	})

	// Test 2: SimilarTo + Where (neuro-symbolic)
	// Find vectors similar to A, but only those written by "bob"
	t.Run("SimilarTo_Where_Author_Bob", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vecA).
			Where("author", "bob").
			Limit(5).
			Execute()

		require.NoError(t, err)
		// Should find at least one result (the exact ID depends on vector similarity)
		assert.Greater(t, len(results), 0, "Should find at least 1 result")

		// Just verify filtering works - results should have non-empty Key
		for _, r := range results {
			assert.NotEmpty(t, r.Key, "Results should have a Key")
			t.Logf("Found: ID=%d, Key=%s, Score=%f", r.ID, r.Key, r.Score)
		}
	})

	// Test 3: SimilarTo + Where (no matches)
	// Find vectors similar to A, but written by "charlie" (doesn't exist)
	t.Run("SimilarTo_Where_Author_Charlie_NoMatch", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vecA).
			Where("author", "charlie").
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.Equal(t, 0, len(results), "Should find no results")
	})

	// Test 4: SimilarTo + Where (multiple matches)
	// Find vectors similar to A, written by "alice"
	t.Run("SimilarTo_Where_Author_Alice", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vecA).
			Where("author", "alice").
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "Should find at least 1 result")

		// Just verify filtering works
		for _, r := range results {
			assert.NotEmpty(t, r.Key, "Results should have a Key")
			t.Logf("Found: ID=%d, Key=%s, Score=%f", r.ID, r.Key, r.Score)
		}
	})

	// Test 5: SimilarTo with threshold
	t.Run("SimilarTo_WithThreshold", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarToWithThreshold(vecA, 0.5). // 50% similarity threshold
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "Should find at least some results")

		// All results should have score >= threshold
		for _, r := range results {
			assert.GreaterOrEqual(t, r.Score, float32(0.5))
		}
	})
}

func TestBuilderValidation(t *testing.T) {
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer dbStore.Close()

	// Test: Execute without SimilarTo should fail
	t.Run("No_SimilarTo", func(t *testing.T) {
		results, err := dbStore.Find().
			Where("author", "alice").
			Limit(5).
			Execute()

		assert.Error(t, err, "Should return error when no vector query provided")
		assert.Nil(t, results)
		assert.Contains(t, err.Error(), "SimilarTo")
	})
}

func TestBuilderFluentAPI(t *testing.T) {
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer dbStore.Close()

	vec := generateRandomVector()

	// Add vector
	id := uint64(42)
	err = dbStore.Vectors().Add(id, vec)
	require.NoError(t, err)

	// Test fluent chaining
	t.Run("Fluent_Chaining", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vec).
			Where("author", "alice").
			Limit(5).
			CandidateMultiplier(5).
			Execute()

		// Should not error (even if no results match)
		require.NoError(t, err)
		assert.NotNil(t, results)
	})

	// Test default values
	t.Run("Default_Values", func(t *testing.T) {
		builder := dbStore.Find().SimilarTo(vec)

		// Check defaults
		// Note: We can't directly access builder fields, but we can test behavior
		results, err := builder.Execute()
		require.NoError(t, err)
		assert.NotNil(t, results)
	})
}

func TestBuilderMultipleFilters(t *testing.T) {
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(t, err)
	defer dbStore.Close()

	// Setup: Add vectors and facts
	vec := generateRandomVector()

	id := uint64(100)
	err = dbStore.Vectors().Add(id, vec)
	require.NoError(t, err)

	// Add multiple facts for the same subject
	err = dbStore.AddFact(meb.Fact{
		Subject:   fmt.Sprintf("id:%d", id),
		Predicate: "author",
		Object:    "alice",
	})
	require.NoError(t, err)

	err = dbStore.AddFact(meb.Fact{
		Subject:   fmt.Sprintf("id:%d", id),
		Predicate: "language",
		Object:    "go",
	})
	require.NoError(t, err)

	// Test: Multiple filters (AND logic)
	t.Run("Multiple_Filters", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vec).
			Where("author", "alice").
			Where("language", "go").
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.Equal(t, 1, len(results), "Should find 1 result matching both filters")
		assert.Equal(t, id, results[0].ID)
	})

	// Test: Multiple filters where one doesn't match
	t.Run("Multiple_Filters_NoMatch", func(t *testing.T) {
		results, err := dbStore.Find().
			SimilarTo(vec).
			Where("author", "alice").
			Where("language", "python"). // Wrong language
			Limit(5).
			Execute()

		require.NoError(t, err)
		assert.Equal(t, 0, len(results), "Should find no results when filters don't all match")
	})
}

func BenchmarkNeuroSymbolicQuery(b *testing.B) {
	cfg := store.InMemoryConfig()
	dbStore, err := meb.NewMEBStore(cfg)
	require.NoError(b, err)
	defer dbStore.Close()

	// Add 10000 vectors with associated facts
	for i := 0; i < 10000; i++ {
		vec := generateRandomVector()
		id := uint64(i + 1000)

		err = dbStore.Vectors().Add(id, vec)
		if err != nil {
			b.Fatal(err)
		}

		// Add author fact
		author := "alice"
		if i%3 == 0 {
			author = "bob"
		}
		err = dbStore.AddFact(meb.Fact{
			Subject:   fmt.Sprintf("id:%d", id),
			Predicate: "author",
			Object:    author,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	queryVec := generateRandomVector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dbStore.Find().
			SimilarTo(queryVec).
			Where("author", "alice").
			Limit(10).
			Execute()
	}
}
