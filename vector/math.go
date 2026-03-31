package vector

import (
	"fmt"
	"math"
)

const FullDim = 1536

func L2Normalize(vec []float32) []float32 {
	var sumSquares float32
	for _, v := range vec {
		sumSquares += v * v
	}

	magnitude := float32(math.Sqrt(float64(sumSquares)))

	result := make([]float32, len(vec))
	if magnitude < 1e-10 {
		return result
	}

	invMag := 1.0 / magnitude
	for i, v := range vec {
		result[i] = v * invMag
	}

	return result
}

func DotProduct(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		panic(fmt.Sprintf("DotProduct: dimension mismatch: len(v1)=%d, len(v2)=%d", len(v1), len(v2)))
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
