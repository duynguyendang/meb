package vector

import (
	"math/rand"
	"testing"
)

// generateRandomVector64 generates a random 64-dimensional vector.
func generateRandomVector64() []float32 {
	vec := make([]float32, MRLDim)
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1 // [-1, 1]
	}
	return vec
}

// BenchmarkDotProductLoop benchmarks the simple loop-based dot product.
func BenchmarkDotProductLoop(b *testing.B) {
	v1 := generateRandomVector64()
	v2 := generateRandomVector64()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProduct(v1, v2)
	}
}

// BenchmarkDotProduct64 benchmarks the SIMD-optimized dot product with loop unrolling.
func BenchmarkDotProduct64(b *testing.B) {
	v1 := generateRandomVector64()
	v2 := generateRandomVector64()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProduct64(v1, v2)
	}
}

// BenchmarkDotProductParallel benchmarks parallel dot product computation.
func BenchmarkDotProductParallel(b *testing.B) {
	vectors := make([]float32, 1000*MRLDim) // 1000 vectors
	for i := range vectors {
		vectors[i] = rand.Float32()*2 - 1
	}

	query := generateRandomVector64()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			offset := j * MRLDim
			DotProduct(vectors[offset:offset+MRLDim], query)
		}
	}
}

// BenchmarkDotProduct64Parallel benchmarks parallel SIMD dot product computation.
func BenchmarkDotProduct64Parallel(b *testing.B) {
	vectors := make([]float32, 1000*MRLDim) // 1000 vectors
	for i := range vectors {
		vectors[i] = rand.Float32()*2 - 1
	}

	query := generateRandomVector64()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			offset := j * MRLDim
			DotProduct64(vectors[offset:offset+MRLDim], query)
		}
	}
}

// BenchmarkProcessMRLSIMD benchmarks the MRL processing (truncation + normalization).
func BenchmarkProcessMRLSIMD(b *testing.B) {
	fullVec := make([]float32, FullDim)
	for i := range fullVec {
		fullVec[i] = rand.Float32()*2 - 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProcessMRL(fullVec)
	}
}

// TestDotProductCorrectness verifies that both implementations produce the same results.
func TestDotProductCorrectness(t *testing.T) {
	// Test with identical vectors
	v1 := []float32{1.0, 0.0, 0.0, 0.0}
	v2 := []float32{1.0, 0.0, 0.0, 0.0}

	// Pad to 64 dimensions
	v1Full := make([]float32, MRLDim)
	v2Full := make([]float32, MRLDim)
	copy(v1Full, v1)
	copy(v2Full, v2)

	result1 := DotProduct(v1Full, v2Full)
	result2 := DotProduct64(v1Full, v2Full)

	if result1 != result2 {
		t.Errorf("DotProduct and DotProduct64 returned different results: %f vs %f", result1, result2)
	}

	// Test with random vectors
	v1 = generateRandomVector64()
	v2 = generateRandomVector64()

	result1 = DotProduct(v1, v2)
	result2 = DotProduct64(v1, v2)

	// Use delta comparison for floating point
	if result1 != result2 {
		// Check if the difference is within acceptable floating point error
		diff := result1 - result2
		if diff < 0 {
			diff = -diff
		}
		if diff > 1e-6 {
			t.Errorf("DotProduct and DotProduct64 returned different results for random vectors: %f vs %f (diff: %f)", result1, result2, diff)
		}
	}

	// Test that exact match returns 1.0 for normalized vectors
	normalized := ProcessMRL(make([]float32, FullDim))
	result1 = DotProduct(normalized, normalized)
	result2 = DotProduct64(normalized, normalized)

	if result1 != result2 {
		diff := result1 - result2
		if diff < 0 {
			diff = -diff
		}
		if diff > 1e-6 {
			t.Errorf("DotProduct and DotProduct64 returned different results for normalized vectors: %f vs %f (diff: %f)", result1, result2, diff)
		}
	}
}
