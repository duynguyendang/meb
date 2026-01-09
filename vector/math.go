package vector

import (
	"math"
)

const (
	// FullDim is the dimension of full vectors (OpenAI embedding standard).
	FullDim = 1536

	// MRLDim is the truncated dimension for MRL search.
	MRLDim = 64
)

// ProcessMRL truncates a full vector to MRLDim dimensions and L2 normalizes it.
// This is used to create the compressed 64-dimensional vectors for fast search.
func ProcessMRL(fullVec []float32) []float32 {
	if len(fullVec) < MRLDim {
		// If vector is smaller than MRLDim, pad with zeros
		result := make([]float32, MRLDim)
		copy(result, fullVec)
		return l2Normalize(result)
	}

	// Truncate to MRLDim
	result := make([]float32, MRLDim)
	copy(result, fullVec[:MRLDim])

	// L2 Normalize
	return l2Normalize(result)
}

// l2Normalize normalizes a vector in-place using L2 norm.
// Returns the same slice for convenience.
func l2Normalize(vec []float32) []float32 {
	// Calculate magnitude
	var sumSquares float32
	for _, v := range vec {
		sumSquares += v * v
	}

	magnitude := float32(math.Sqrt(float64(sumSquares)))

	// Handle division by zero
	if magnitude < 1e-10 {
		// Return zero vector if magnitude is too small
		for i := range vec {
			vec[i] = 0
		}
		return vec
	}

	// Normalize
	invMag := 1.0 / magnitude
	for i := range vec {
		vec[i] *= invMag
	}

	return vec
}

// DotProduct calculates the dot product of two vectors.
// Since vectors are L2 normalized, this equals cosine similarity.
func DotProduct(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return 0
	}

	var sum float32
	for i := range v1 {
		sum += v1[i] * v2[i]
	}

	return sum
}

// CosineSimilarity calculates the cosine similarity between two vectors.
// For normalized vectors, this is equivalent to DotProduct.
func CosineSimilarity(v1, v2 []float32) float32 {
	return DotProduct(v1, v2)
}
