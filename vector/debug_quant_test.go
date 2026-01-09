package vector

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDebugQuantizationIssue debugs the negative score issue.
func TestDebugQuantizationIssue(t *testing.T) {
	rand.Seed(42)

	// Generate a random vector (same as the query test)
	vec := make([]float32, FullDim)
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1 // [-1, 1]
	}

	// Process MRL (truncate and normalize)
	mrl := ProcessMRL(vec)

	// Check if MRL is normalized
	var sumSquares float32
	for _, v := range mrl {
		sumSquares += v * v
	}
	fmt.Printf("MRL magnitude squared: %f\n", sumSquares)

	// Quantize
	quantized := Quantize(mrl)
	fmt.Printf("Quantized first 10 values: %v\n", quantized[:10])

	// Compute dot product of MRL with itself (should be ~1.0)
	score1 := DotProduct64(mrl, mrl)
	fmt.Printf("Dot product (float32): %f\n", score1)

	// Compute dot product of quantized with itself
	score2 := DotProductInt8(quantized, quantized)
	fmt.Printf("Dot product (int8): %f\n", score2)

	// They should both be close to 1.0
	assert.InDelta(t, 1.0, float64(score1), 0.01, "Float32 dot product should be ~1.0")
	assert.InDelta(t, 1.0, float64(score2), 0.05, "Int8 dot product should be ~1.0")
}
