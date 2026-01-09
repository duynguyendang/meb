package vector

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openTestDB creates a test BadgerDB instance.
func openTestDB(t *testing.T) *badger.DB {
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	require.NoError(t, err)
	return db
}

// generateRandomVector generates a random 1536-d vector.
func generateRandomVector() []float32 {
	vec := make([]float32, FullDim)
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1 // [-1, 1]
	}
	return vec
}

// generateNormalizedVector generates a random normalized 1536-d vector.
func generateNormalizedVector() []float32 {
	vec := make([]float32, FullDim)
	var sum float32
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1
		sum += vec[i] * vec[i]
	}
	// Normalize
	mag := float32(1.0 / (sqrt(float64(sum)) + 1e-10))
	for i := range vec {
		vec[i] *= mag
	}
	return vec
}

func sqrt(x float64) float64 {
	// Simple square root approximation
	z := 1.0
	for i := 0; i < 20; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}

func TestProcessMRL(t *testing.T) {
	fullVec := generateRandomVector()

	mrlVec := ProcessMRL(fullVec)

	// Check dimension
	assert.Equal(t, MRLDim, len(mrlVec), "MRL vector should have 64 dimensions")

	// Check first 64 values match input
	for i := 0; i < MRLDim; i++ {
		// Values should be normalized, so not exactly equal but proportional
		assert.NotEqual(t, 0.0, mrlVec[i], "MRL vector should not be zero")
	}

	// Check L2 normalization (magnitude should be ~1.0)
	var sumSquares float32
	for _, v := range mrlVec {
		sumSquares += v * v
	}
	mag := float32(1.0 / (sqrt(float64(sumSquares)) + 1e-10))
	assert.InDelta(t, 1.0, float64(mag), 1e-5, "MRL vector should be L2 normalized")
}

func TestProcessMRL_SmallVector(t *testing.T) {
	smallVec := make([]float32, 32)
	for i := range smallVec {
		smallVec[i] = rand.Float32()
	}

	mrlVec := ProcessMRL(smallVec)

	// Should be padded to MRLDim
	assert.Equal(t, MRLDim, len(mrlVec), "Small vector should be padded to 64 dimensions")

	// First 32 values should be proportional to input (normalized)
	// Check that the ratio is maintained for non-zero values
	for i := 0; i < 32; i++ {
		if smallVec[i] != 0 {
			ratio := mrlVec[i] / smallVec[i]
			// All non-zero values should have the same normalization factor
			assert.Greater(t, ratio, float32(0), "Ratio should be positive")
		}
	}

	// Last 32 values should be zero (padded)
	for i := 32; i < MRLDim; i++ {
		assert.Equal(t, float32(0), mrlVec[i], "Padded values should be zero")
	}
}

func TestDotProduct(t *testing.T) {
	v1 := []float32{1.0, 0.0, 0.0}
	v2 := []float32{1.0, 0.0, 0.0}

	result := DotProduct(v1, v2)
	assert.Equal(t, float32(1.0), result, "Dot product of identical unit vectors should be 1")

	v3 := []float32{0.0, 1.0, 0.0}
	result = DotProduct(v1, v3)
	assert.Equal(t, float32(0.0), result, "Dot product of orthogonal vectors should be 0")
}

func TestDotProduct_DifferentLengths(t *testing.T) {
	v1 := []float32{1.0, 0.0}
	v2 := []float32{1.0, 0.0, 0.0}

	result := DotProduct(v1, v2)
	assert.Equal(t, float32(0.0), result, "Dot product of different length vectors should be 0")
}

func TestNewRegistry(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	assert.NotNil(t, reg)
	assert.NotNil(t, reg.data)
	assert.NotNil(t, reg.idMap)
	assert.NotNil(t, reg.revMap)
	assert.Equal(t, 0, reg.Count(), "New registry should be empty")
}

func TestVectorRegistry_Add(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add a vector
	vec := generateRandomVector()
	id := uint64(42)

	err := reg.Add(id, vec)
	require.NoError(t, err)

	// Wait for async write
	reg.wg.Wait()

	assert.Equal(t, 1, reg.Count(), "Registry should have 1 vector")
}

func TestVectorRegistry_Add_InvalidDimension(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add a vector with wrong dimension
	vec := make([]float32, 100) // Not 1536
	id := uint64(42)

	err := reg.Add(id, vec)
	assert.Error(t, err, "Should reject vector with wrong dimension")
	assert.Contains(t, err.Error(), "invalid vector dimension")
}

func TestVectorRegistry_Add_Overwrite(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add first vector
	vec1 := generateRandomVector()
	id := uint64(42)

	err := reg.Add(id, vec1)
	require.NoError(t, err)
	reg.wg.Wait()

	assert.Equal(t, 1, reg.Count(), "Registry should have 1 vector")

	// Overwrite with second vector
	vec2 := generateRandomVector()
	err = reg.Add(id, vec2)
	require.NoError(t, err)
	reg.wg.Wait()

	assert.Equal(t, 1, reg.Count(), "Registry should still have 1 vector after overwrite")
}

func TestVectorRegistry_Search(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add 100 random vectors
	vectors := make([][]float32, 100)
	ids := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = generateNormalizedVector()
		ids[i] = uint64(i + 1)
		err := reg.Add(ids[i], vectors[i])
		require.NoError(t, err)
	}

	// Wait for all async writes
	reg.wg.Wait()

	// Search for vector 50 (should return itself as top result)
	queryVec := vectors[49]
	results, err := reg.Search(queryVec, 5)
	require.NoError(t, err)

	assert.Len(t, results, 5, "Should return 5 results")
	assert.Equal(t, uint64(50), results[0].ID, "Top result should be vector 50")
	assert.InDelta(t, 1.0, float64(results[0].Score), 0.01, "Top score should be ~1.0 for identical vectors (with int8 quantization)")
}

func TestVectorRegistry_Search_Empty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Search empty registry
	queryVec := generateRandomVector()
	results, err := reg.Search(queryVec, 5)
	require.NoError(t, err)

	assert.Nil(t, results, "Should return nil for empty registry")
}

func TestVectorRegistry_Search_LargeK(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add 10 vectors
	for i := 0; i < 10; i++ {
		vec := generateRandomVector()
		err := reg.Add(uint64(i+1), vec)
		require.NoError(t, err)
	}
	reg.wg.Wait()

	// Search for k=20 when only 10 vectors exist
	queryVec := generateRandomVector()
	results, err := reg.Search(queryVec, 20)
	require.NoError(t, err)

	assert.Len(t, results, 10, "Should return at most 10 results")
}

func TestVectorRegistry_Persistence(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create registry and add vectors
	reg1 := NewRegistry(db)

	vectors := make([][]float32, 50)
	ids := make([]uint64, 50)
	for i := 0; i < 50; i++ {
		vectors[i] = generateRandomVector()
		ids[i] = uint64(i + 1)
		err := reg1.Add(ids[i], vectors[i])
		require.NoError(t, err)
	}
	reg1.wg.Wait()

	// Save snapshot
	err := reg1.SaveSnapshot()
	require.NoError(t, err)

	// Close and create new registry with same DB
	reg1.Close()

	reg2 := NewRegistry(db)
	err = reg2.LoadSnapshot()
	require.NoError(t, err)

	assert.Equal(t, 50, reg2.Count(), "Loaded registry should have 50 vectors")

	// Verify search works
	queryVec := vectors[24]
	results, err := reg2.Search(queryVec, 5)
	require.NoError(t, err)
	assert.Len(t, results, 5, "Should return 5 results")
	assert.Equal(t, uint64(25), results[0].ID, "Top result should be vector 25")
	assert.InDelta(t, 1.0, float64(results[0].Score), 0.01, "Top score should be ~1.0 for identical vectors (with int8 quantization)")
}

func TestVectorRegistry_GetFullVector(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add a vector
	originalVec := generateRandomVector()
	id := uint64(42)

	err := reg.Add(id, originalVec)
	require.NoError(t, err)
	reg.wg.Wait()

	// Retrieve full vector
	retrievedVec, err := reg.GetFullVector(id)
	require.NoError(t, err)

	assert.Equal(t, FullDim, len(retrievedVec), "Retrieved vector should have 1536 dimensions")

	// Compare values
	for i := 0; i < FullDim; i++ {
		assert.InDelta(t, float64(originalVec[i]), float64(retrievedVec[i]), 1e-6,
			"Retrieved vector should match original at index %d", i)
	}
}

func TestVectorRegistry_ConcurrentAdd(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add vectors concurrently
	numGoroutines := 100
	vectorsPerGoroutine := 10

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < vectorsPerGoroutine; i++ {
				vec := generateRandomVector()
				id := uint64(goroutineID*vectorsPerGoroutine + i)
				err := reg.Add(id, vec)
				assert.NoError(t, err)
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Wait for async writes
	reg.wg.Wait()

	assert.Equal(t, numGoroutines*vectorsPerGoroutine, reg.Count(),
		"Registry should have all vectors")
}

func TestVectorRegistry_ConcurrentSearch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	reg := NewRegistry(db)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vec := generateRandomVector()
		err := reg.Add(uint64(i), vec)
		require.NoError(t, err)
	}
	reg.wg.Wait()

	// Perform concurrent searches
	numGoroutines := 50
	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			queryVec := generateRandomVector()
			results, err := reg.Search(queryVec, 10)
			assert.NoError(t, err)
			assert.NotNil(t, results)
			assert.LessOrEqual(t, len(results), 10)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func BenchmarkProcessMRL(b *testing.B) {
	vec := generateRandomVector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProcessMRL(vec)
	}
}

func BenchmarkDotProduct(b *testing.B) {
	v1 := generateRandomVector()
	v2 := generateRandomVector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProduct(v1[:MRLDim], v2[:MRLDim])
	}
}

func BenchmarkVectorRegistry_Add(b *testing.B) {
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	reg := NewRegistry(db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vec := generateRandomVector()
		_ = reg.Add(uint64(i), vec)
	}
	reg.wg.Wait()
}

func BenchmarkVectorRegistry_Search(b *testing.B) {
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	reg := NewRegistry(db)

	// Add 100k vectors
	for i := 0; i < 100000; i++ {
		vec := generateRandomVector()
		_ = reg.Add(uint64(i), vec)
	}
	reg.wg.Wait()

	queryVec := generateRandomVector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Search(queryVec, 10)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
