package vector

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// TestADCAccumEquivalence verifies that the dispatched adcAccum function
// produces the same result as the canonical scalar implementation.
func TestADCAccumEquivalence(t *testing.T) {
	tests := []struct {
		name         string
		numSubSpaces int
		seed         int64
	}{
		{"small_2", 2, 42},
		{"typical_16", 16, 42},
		{"typical_32", 32, 42},
		{"typical_64", 64, 42},
		{"large_128", 128, 42},
		{"odd_7", 7, 42},
		{"odd_13", 13, 42},
		{"odd_31", 31, 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(tt.seed))
			lut := make([]float32, tt.numSubSpaces*256)
			for i := range lut {
				lut[i] = rng.Float32() * 10
			}
			codes := make([]byte, tt.numSubSpaces)
			for i := range codes {
				codes[i] = byte(rng.Intn(256))
			}

			scalar := adcAccumScalar(lut, codes)
			dispatched := adcAccum(lut, codes)

			// Allow small FP tolerance due to different accumulation order
			tol := float32(1e-4) * float32(tt.numSubSpaces)
			if math.Abs(float64(scalar-dispatched)) > float64(tol) {
				t.Errorf("adcAccum = %f, scalar = %f, diff = %f (tol = %f)",
					dispatched, scalar, math.Abs(float64(scalar-dispatched)), tol)
			}
		})
	}
}

// TestADCAccumEmpty verifies behavior with empty input.
func TestADCAccumEmpty(t *testing.T) {
	lut := make([]float32, 0)
	codes := make([]byte, 0)

	scalar := adcAccumScalar(lut, codes)
	dispatched := adcAccum(lut, codes)

	if scalar != 0 {
		t.Errorf("scalar empty = %f, want 0", scalar)
	}
	if dispatched != 0 {
		t.Errorf("dispatched empty = %f, want 0", dispatched)
	}
}

// TestADCAccumSingle verifies behavior with a single subspace.
func TestADCAccumSingle(t *testing.T) {
	lut := make([]float32, 256)
	for i := range lut {
		lut[i] = float32(i) * 0.1
	}
	codes := []byte{42}

	scalar := adcAccumScalar(lut, codes)
	dispatched := adcAccum(lut, codes)
	expected := lut[42]

	if scalar != expected {
		t.Errorf("scalar = %f, want %f", scalar, expected)
	}
	if dispatched != expected {
		t.Errorf("dispatched = %f, want %f", dispatched, expected)
	}
}

// TestADCAccumDispatch verifies that adcAccum is not nil after init.
func TestADCAccumDispatch(t *testing.T) {
	if adcAccum == nil {
		t.Fatal("adcAccum is nil after init")
	}

	lut := make([]float32, 4*256)
	for i := range lut {
		lut[i] = float32(i % 256)
	}
	codes := []byte{0, 1, 2, 3}

	result := adcAccum(lut, codes)
	expected := float32(0 + 1 + 2 + 3)
	if result != expected {
		t.Errorf("adcAccum = %f, want %f", result, expected)
	}
}

// BenchmarkADCAccumScalar benchmarks the scalar implementation.
func BenchmarkADCAccumScalar(b *testing.B) {
	for _, numSubSpaces := range []int{16, 32, 64, 128} {
		name := fmt.Sprintf("sub%d", numSubSpaces)
		b.Run(name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			lut := make([]float32, numSubSpaces*256)
			for i := range lut {
				lut[i] = rng.Float32()
			}
			codes := make([]byte, numSubSpaces)
			for i := range codes {
				codes[i] = byte(rng.Intn(256))
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = adcAccumScalar(lut, codes)
			}
			b.SetBytes(int64(numSubSpaces * 4)) // LUT reads: numSubSpaces * 4 bytes each
		})
	}
}

// BenchmarkADCAccumDispatched benchmarks the dispatched (platform-optimized) implementation.
func BenchmarkADCAccumDispatched(b *testing.B) {
	for _, numSubSpaces := range []int{16, 32, 64, 128} {
		name := fmt.Sprintf("sub%d", numSubSpaces)
		b.Run(name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			lut := make([]float32, numSubSpaces*256)
			for i := range lut {
				lut[i] = rng.Float32()
			}
			codes := make([]byte, numSubSpaces)
			for i := range codes {
				codes[i] = byte(rng.Intn(256))
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = adcAccum(lut, codes)
			}
			b.SetBytes(int64(numSubSpaces * 4))
		})
	}
}


