package vector

import (
	"math"
	"testing"
)

func TestDefaultTurboQuantConfig(t *testing.T) {
	cfg := DefaultTurboQuantConfig()
	if cfg.BitWidth != 8 {
		t.Errorf("expected bit width 8, got %d", cfg.BitWidth)
	}
	if cfg.BlockSize != 32 {
		t.Errorf("expected block size 32, got %d", cfg.BlockSize)
	}
}

func TestTQVectorSize(t *testing.T) {
	cfg := DefaultTurboQuantConfig()
	size := TQVectorSize(1536, cfg)
	if size <= 0 {
		t.Errorf("expected positive vector size, got %d", size)
	}
}

func TestQuantizeDequantize8Bit(t *testing.T) {
	cfg := &TurboQuantConfig{BitWidth: 8, BlockSize: 32}
	dim := 128
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) / float32(dim)
	}

	quantized := QuantizeTurboQuant(vec, cfg)
	if len(quantized) == 0 {
		t.Fatal("quantized data is empty")
	}

	dequantized := DequantizeTurboQuant(quantized, dim, cfg)
	if len(dequantized) != dim {
		t.Fatalf("dequantized length = %d, want %d", len(dequantized), dim)
	}

	for i := range dequantized {
		diff := math.Abs(float64(vec[i] - dequantized[i]))
		if diff > 0.05 {
			t.Errorf("element %d: diff %.6f too large (original=%.6f, dequant=%.6f)", i, diff, vec[i], dequantized[i])
		}
	}
}

func TestQuantizeDequantize4Bit(t *testing.T) {
	cfg := &TurboQuantConfig{BitWidth: 4, BlockSize: 32}
	dim := 128
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) / float32(dim)
	}

	quantized := QuantizeTurboQuant(vec, cfg)
	if len(quantized) == 0 {
		t.Fatal("quantized data is empty")
	}

	dequantized := DequantizeTurboQuant(quantized, dim, cfg)
	if len(dequantized) != dim {
		t.Fatalf("dequantized length = %d, want %d", len(dequantized), dim)
	}
}

func TestDotProductTurboQuant(t *testing.T) {
	cfg := &TurboQuantConfig{BitWidth: 8, BlockSize: 32}
	dim := 128

	vecA := make([]float32, dim)
	vecB := make([]float32, dim)
	for i := range vecA {
		vecA[i] = float32(i) / float32(dim)
		vecB[i] = float32(dim-i) / float32(dim)
	}

	qA := QuantizeTurboQuant(vecA, cfg)
	qB := QuantizeTurboQuant(vecB, cfg)

	tqDot := DotProductTurboQuant(qA, qB, dim, cfg)

	// Compute expected dot product
	var expected float32
	for i := range vecA {
		expected += vecA[i] * vecB[i]
	}

	diff := math.Abs(float64(tqDot - expected))
	if diff > 0.05 {
		t.Errorf("dot product mismatch: tq=%.6f, expected=%.6f, diff=%.6f", tqDot, expected, diff)
	}
}

func TestL2Normalize(t *testing.T) {
	vec := []float32{3.0, 4.0}
	normalized := L2Normalize(vec)

	var norm float32
	for _, v := range normalized {
		norm += v * v
	}
	if math.Abs(float64(norm-1.0)) > 0.001 {
		t.Errorf("expected norm 1.0, got %f", norm)
	}
}

func TestCosineSimilarity(t *testing.T) {
	vec := []float32{1.0, 0.0, 0.0}
	normalized := L2Normalize(vec)
	sim := CosineSimilarity(normalized, normalized)
	if math.Abs(float64(sim-1.0)) > 0.001 {
		t.Errorf("expected similarity 1.0 for identical vectors, got %f", sim)
	}
}

func TestL2NormalizeZero(t *testing.T) {
	vec := []float32{0.0, 0.0, 0.0}
	normalized := L2Normalize(vec)
	for i, v := range normalized {
		if v != 0.0 {
			t.Errorf("expected 0 at index %d, got %f", i, v)
		}
	}
}
