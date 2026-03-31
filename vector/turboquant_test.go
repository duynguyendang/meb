package vector

import (
	"math"
	"testing"
)

func TestDefaultHybridConfig(t *testing.T) {
	cfg := DefaultHybridConfig()
	if cfg.BitWidth != 8 {
		t.Errorf("expected bit width 8, got %d", cfg.BitWidth)
	}
	if cfg.BlockSize != 32 {
		t.Errorf("expected block size 32, got %d", cfg.BlockSize)
	}
}

func TestHybridVectorSize(t *testing.T) {
	cfg := DefaultHybridConfig()
	size := HybridVectorSize(1536, cfg)
	if size <= 0 {
		t.Errorf("expected positive vector size, got %d", size)
	}
}

func TestFWHT(t *testing.T) {
	vec := []float32{1.0, 2.0, 3.0, 4.0}
	FWHT(vec)

	FWHT(vec)
	for i, v := range vec {
		expected := []float32{1.0, 2.0, 3.0, 4.0}[i] * 4.0
		if math.Abs(float64(v-expected)) > 1e-5 {
			t.Errorf("FWHT inverse mismatch at %d: got %f, expected %f", i, v, expected)
		}
	}
}

func TestNextPow2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{1536, 2048},
	}
	for _, tt := range tests {
		if got := nextPow2(tt.input); got != tt.expected {
			t.Errorf("nextPow2(%d) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestQuantizeDequantizeHybrid8Bit(t *testing.T) {
	cfg := &HybridConfig{BitWidth: 8, BlockSize: 32}
	dim := 128
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) / float32(dim)
	}

	quantized := QuantizeHybrid(vec, cfg)
	if len(quantized) == 0 {
		t.Fatal("quantized data is empty")
	}

	dequantized := DequantizeHybrid(quantized, dim, cfg)
	if len(dequantized) != dim {
		t.Fatalf("dequantized length = %d, want %d", len(dequantized), dim)
	}

	var origNorm, reconNorm, dotProd float64
	for i := range vec {
		origNorm += float64(vec[i] * vec[i])
		reconNorm += float64(dequantized[i] * dequantized[i])
		dotProd += float64(vec[i] * dequantized[i])
	}
	cosine := dotProd / (math.Sqrt(origNorm) * math.Sqrt(reconNorm))
	if cosine < 0.99 {
		t.Errorf("cosine similarity %.6f too low for 8-bit Hybrid", cosine)
	}
}

func TestQuantizeDequantizeHybrid4Bit(t *testing.T) {
	cfg := &HybridConfig{BitWidth: 4, BlockSize: 32}
	dim := 128
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) / float32(dim)
	}

	quantized := QuantizeHybrid(vec, cfg)
	dequantized := DequantizeHybrid(quantized, dim, cfg)

	var origNorm, reconNorm, dotProd float64
	for i := range vec {
		origNorm += float64(vec[i] * vec[i])
		reconNorm += float64(dequantized[i] * dequantized[i])
		dotProd += float64(vec[i] * dequantized[i])
	}
	cosine := dotProd / (math.Sqrt(origNorm) * math.Sqrt(reconNorm))
	if cosine < 0.95 {
		t.Errorf("cosine similarity %.6f too low for 4-bit Hybrid", cosine)
	}
}

func TestDotProductHybrid(t *testing.T) {
	cfg := &HybridConfig{BitWidth: 8, BlockSize: 32}
	dim := 128

	vecA := make([]float32, dim)
	vecB := make([]float32, dim)
	for i := range vecA {
		vecA[i] = float32(i) / float32(dim)
		vecB[i] = float32(dim-i) / float32(dim)
	}

	qA := QuantizeHybrid(vecA, cfg)
	qB := QuantizeHybrid(vecB, cfg)

	hybridDot := DotProductHybrid(qA, qB, dim, cfg)

	var expected float32
	for i := range vecA {
		expected += vecA[i] * vecB[i]
	}

	diff := math.Abs(float64(hybridDot - expected))
	if diff > 0.5 {
		t.Errorf("dot product mismatch: hybrid=%.6f, expected=%.6f, diff=%.6f", hybridDot, expected, diff)
	}
}

func TestDotProductHybridSelfSimilarity(t *testing.T) {
	cfg := DefaultHybridConfig()
	dim := 128

	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) / float32(dim)
	}

	q := QuantizeHybrid(vec, cfg)
	score := DotProductHybrid(q, q, dim, cfg)

	if score <= 0 {
		t.Errorf("self-similarity should be positive, got %f", score)
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

func TestL2NormalizeNoMutation(t *testing.T) {
	original := []float32{3.0, 4.0}
	normalized := L2Normalize(original)

	if original[0] != 3.0 || original[1] != 4.0 {
		t.Error("L2Normalize mutated input slice")
	}

	var norm float32
	for _, v := range normalized {
		norm += v * v
	}
	if math.Abs(float64(norm-1.0)) > 0.001 {
		t.Errorf("expected norm 1.0, got %f", norm)
	}
}
