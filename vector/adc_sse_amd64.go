//go:build amd64

package vector

// adcAccumSSE is an SSE-accelerated ADC accumulator implemented in
// adc_sse_amd64.s. It processes 4 subspaces per iteration using SSE
// scalar-gather + vector-accumulate, with a scalar tail for remainders.
func adcAccumSSE(lut []float32, codes []byte) float32
