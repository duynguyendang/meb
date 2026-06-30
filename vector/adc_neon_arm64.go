//go:build arm64

package vector

// adcAccumNEON is a NEON-accelerated ADC accumulator implemented in
// adc_neon_arm64.s. It processes 4 subspaces per iteration using 4
// independent scalar accumulators (FADDS) to hide memory latency,
// with a scalar tail for remainders.
func adcAccumNEON(lut []float32, codes []byte) float32
