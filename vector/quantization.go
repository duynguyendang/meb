package vector

import "math"

// Quantize converts a normalized float32 vector [-1, 1] to int8 [-127, 127].
// This provides 4x compression (32 bits -> 8 bits) with minimal accuracy loss.
func Quantize(vec []float32) []int8 {
	res := make([]int8, len(vec))
	for i, v := range vec {
		// Clamp to [-1, 1] to handle any floating point precision issues
		if v > 1.0 {
			v = 1.0
		} else if v < -1.0 {
			v = -1.0
		}
		// Scale to [-127, 127] and round to nearest integer
		res[i] = int8(math.Round(float64(v * 127)))
	}
	return res
}

// Dequantize converts an int8 vector back to float32 [-1, 1].
// This is useful for debugging and verification.
func Dequantize(vec []int8) []float32 {
	res := make([]float32, len(vec))
	for i, v := range vec {
		res[i] = float32(v) / 127.0
	}
	return res
}
