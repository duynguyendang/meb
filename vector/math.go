package vector

import (
	"math"
)

const (
	FullDim = 1536
	MRLDim  = 64
)

func ProcessMRL(fullVec []float32, mrlDim int) []float32 {
	if len(fullVec) < mrlDim {
		result := make([]float32, mrlDim)
		copy(result, fullVec)
		return l2Normalize(result)
	}

	result := make([]float32, mrlDim)
	copy(result, fullVec[:mrlDim])

	return l2Normalize(result)
}

func l2Normalize(vec []float32) []float32 {
	var sumSquares float32
	for _, v := range vec {
		sumSquares += v * v
	}

	magnitude := float32(math.Sqrt(float64(sumSquares)))

	if magnitude < 1e-10 {
		for i := range vec {
			vec[i] = 0
		}
		return vec
	}

	invMag := 1.0 / magnitude
	for i := range vec {
		vec[i] *= invMag
	}

	return vec
}

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

func CosineSimilarity(v1, v2 []float32) float32 {
	return DotProduct(v1, v2)
}
