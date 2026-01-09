package vector

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestQuantization tests quantization and dequantization.
func TestQuantization(t *testing.T) {
	// Test with known values
	vec := []float32{1.0, 0.5, 0.0, -0.5, -1.0}
	quantized := Quantize(vec)

	assert.Equal(t, []int8{127, 64, 0, -64, -127}, quantized)

	// Test dequantization
	dequantized := Dequantize(quantized)
	assert.InDelta(t, 1.0, float64(dequantized[0]), 0.01)
	assert.InDelta(t, 0.5, float64(dequantized[1]), 0.01)
	assert.InDelta(t, 0.0, float64(dequantized[2]), 0.01)
	assert.InDelta(t, -0.5, float64(dequantized[3]), 0.01)
	assert.InDelta(t, -1.0, float64(dequantized[4]), 0.01)
}

// TestDotProductInt8_Correctness tests that int8 dot product produces similar results.
func TestDotProductInt8_Correctness(t *testing.T) {
	// Generate normalized vectors
	v1 := generateNormalizedVector64()
	v2 := generateNormalizedVector64()

	// Compute with float32
	scoreFloat := DotProduct64(v1, v2)

	// Quantize and compute with int8
	v1q := Quantize(v1)
	v2q := Quantize(v2)
	scoreInt := DotProductInt8(v1q, v2q)

	// Should be similar (within 5% due to quantization error)
	diff := scoreFloat - scoreInt
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, float64(diff), 0.05, "Quantization error should be less than 5%%")
}

// TestDotProductInt8_ExactMatch tests that exact matches still work with int8.
func TestDotProductInt8_ExactMatch(t *testing.T) {
	// Create identical vectors
	vec := generateNormalizedVector64()

	scoreFloat := DotProduct64(vec, vec)

	// Quantize and test
	quantized := Quantize(vec)
	scoreInt := DotProductInt8(quantized, quantized)

	// Both should be very close to 1.0
	assert.InDelta(t, 1.0, float64(scoreFloat), 0.01)
	assert.InDelta(t, 1.0, float64(scoreInt), 0.05)
}

// TestInt8RankingPreservation tests that int8 preserves ranking order.
func TestInt8RankingPreservation(t *testing.T) {
	// Create a query vector
	query := generateNormalizedVector64()

	// Create 100 vectors with varying similarities
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = generateNormalizedVector64()
	}

	// Compute scores with float32
	type scoreIndex struct {
		score float32
		idx   int
	}
	floatScores := make([]scoreIndex, 100)
	for i, vec := range vectors {
		floatScores[i] = scoreIndex{
			score: DotProduct64(query, vec),
			idx:   i,
		}
	}

	// Compute scores with int8
	intScores := make([]scoreIndex, 100)
	for i, vec := range vectors {
		q := Quantize(query)
		v := Quantize(vec)
		intScores[i] = scoreIndex{
			score: DotProductInt8(q, v),
			idx:   i,
		}
	}

	// Sort both by score
	for i := 0; i < len(floatScores); i++ {
		for j := i + 1; j < len(floatScores); j++ {
			if floatScores[j].score > floatScores[i].score {
				floatScores[i], floatScores[j] = floatScores[j], floatScores[i]
			}
		}
	}

	for i := 0; i < len(intScores); i++ {
		for j := i + 1; j < len(intScores); j++ {
			if intScores[j].score > intScores[i].score {
				intScores[i], intScores[j] = intScores[j], intScores[i]
			}
		}
	}

	// Check top-10 ranking preservation
	topK := 10
	matches := 0
	for i := 0; i < topK; i++ {
		for j := 0; j < topK; j++ {
			if floatScores[i].idx == intScores[j].idx {
				matches++
				break
			}
		}
	}

	// At least 80% of top-10 should match
	ratio := float64(matches) / float64(topK)
	assert.Greater(t, ratio, 0.8, "At least 80%% of top-10 rankings should match (got %.0f%%)", ratio*100)
}

// generateNormalizedVector64 generates a random normalized 64-d vector.
func generateNormalizedVector64() []float32 {
	vec := make([]float32, MRLDim)
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

// BenchmarkDotProductInt8 benchmarks the int8 dot product.
func BenchmarkDotProductInt8(b *testing.B) {
	v1 := generateRandomVector64()
	v2 := generateRandomVector64()

	v1q := Quantize(v1)
	v2q := Quantize(v2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DotProductInt8(v1q, v2q)
	}
}

// BenchmarkQuantization benchmarks the quantization process.
func BenchmarkQuantization(b *testing.B) {
	vec := generateRandomVector64()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Quantize(vec)
	}
}
