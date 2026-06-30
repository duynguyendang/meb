// Package vector — ADC accumulation dispatch.
//
// adcAccum is the hot-path function for IVF-PQ asymmetric distance computation.
// It is selected at init() time based on CPU feature detection.
package vector

// adcAccum computes Σ_s lut[s*256 + codes[s]] — the core ADC accumulation.
// The concrete implementation is chosen at first call by platform-specific files:
//   - adc_dispatch_amd64.go: SSE if enabled and beats scalar, else scalar
//   - adc_dispatch_arm64.go: NEON/ASIMD if available and beats scalar, else scalar
//   - adc_dispatch_generic.go: scalar only
var adcAccum func(lut []float32, codes []byte) float32
