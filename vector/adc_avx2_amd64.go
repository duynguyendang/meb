//go:build amd64

package vector

// adcAccumAVX2 is an AVX2-accelerated ADC accumulator (stub, falls back to scalar).
// This will be replaced with actual SIMD intrinsics in a future PR.
func adcAccumAVX2(lut []float32, codes []byte) float32 {
	var dist float32
	for s := 0; s < len(codes); s++ {
		dist += lut[s*256+int(codes[s])]
	}
	return dist
}
