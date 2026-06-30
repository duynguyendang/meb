//go:build amd64

package vector

import (
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/cpu"
)

var (
	adcAccumImpl func(lut []float32, codes []byte) float32
	adcAccumOnce sync.Once
)

func init() {
	// The scalar 4-way unrolled implementation meets or beats SSE on current
	// hardware (≈0.93× ratio for the SIMD kernels due to gather latency
	// and equivalent scalar ILP). The SSE path is therefore opt-in only.
	//
	// Set MEB_AVX2_ADC=1 in the process environment to enable the SSE kernel
	// (historically named; the kernel uses SSE, not AVX2). When enabled, a
	// micro-benchmark still gates it: if SSE does not beat scalar by ≥20%
	// on this CPU, it falls back to scalar automatically.
	adcAccum = adcAccumSelect
}

func adcAccumSelect(lut []float32, codes []byte) float32 {
	adcAccumOnce.Do(selectAdcImpl)
	return adcAccumImpl(lut, codes)
}

func selectAdcImpl() {
	if cpu.X86.HasSSE2 && envEnabled("MEB_AVX2_ADC") {
		beats, _ := sseBeatsScalarEx()
		if beats {
			adcAccumImpl = adcAccumSSE
			return
		}
	}
	adcAccumImpl = adcAccumScalar
}

// envEnabled returns true if the named environment variable is set to "1".
func envEnabled(name string) bool {
	v, _ := strconv.ParseBool(os.Getenv(name))
	return v
}

// sseBeatsScalarEx runs a quick micro-benchmark and returns whether SSE
// beats scalar plus the scalar/sse ratio (ratio > 1 means SSE is faster).
func sseBeatsScalarEx() (bool, float64) {
	const (
		numSubSpaces = 32
		iterations   = 16384
	)
	lut := make([]float32, numSubSpaces*256)
	for i := range lut {
		lut[i] = float32(i%256) * 0.01
	}
	codes := make([]byte, numSubSpaces)
	for i := range codes {
		codes[i] = byte(i * 7)
	}

	for i := 0; i < 256; i++ {
		_ = adcAccumScalar(lut, codes)
		_ = adcAccumSSE(lut, codes)
	}

	var scalarDurs [3]time.Duration
	for run := 0; run < 3; run++ {
		t0 := time.Now()
		sink := float32(0)
		for i := 0; i < iterations; i++ {
			sink += adcAccumScalar(lut, codes)
		}
		scalarDurs[run] = time.Since(t0)
		_ = sink
	}
	scalarDur := medianDuration(scalarDurs[:])

	var sseDurs [3]time.Duration
	for run := 0; run < 3; run++ {
		t0 := time.Now()
		sink := float32(0)
		for i := 0; i < iterations; i++ {
			sink += adcAccumSSE(lut, codes)
		}
		sseDurs[run] = time.Since(t0)
		_ = sink
	}
	sseDur := medianDuration(sseDurs[:])

	ratio := float64(scalarDur) / float64(sseDur)
	return float64(sseDur) < float64(scalarDur)*0.80 && !math.IsNaN(float64(sseDur)), ratio
}

// sseBeatsScalar runs a quick micro-benchmark to decide whether the SSE
// kernel is actually faster than the scalar one on this CPU.
func sseBeatsScalar() bool {
	beats, _ := sseBeatsScalarEx()
	return beats
}

func medianDuration(durs []time.Duration) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	for i := 0; i < len(durs); i++ {
		for j := i + 1; j < len(durs); j++ {
			if durs[j] < durs[i] {
				durs[i], durs[j] = durs[j], durs[i]
			}
		}
	}
	return durs[len(durs)/2]
}
