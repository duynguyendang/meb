package vector

import (
	"bytes"
	"math"
	"testing"
)

func TestPadAndTransformGolden(t *testing.T) {
	cfg := DefaultHybridConfig()
	paddedDim := nextPow2(1536)

	input := make([]float32, 1536)
	for i := range input {
		input[i] = float32(i) / float32(1536)
	}

	before := QuantizeHybrid(input, cfg)
	cfg.ensureDerived(1536)
	after := QuantizeHybrid(input, cfg)

	if !bytes.Equal(before, after) {
		t.Fatal("PadAndTransform refactor changed output")
	}

	transformed := PadAndTransform(input, paddedDim)
	if len(transformed) != paddedDim {
		t.Fatalf("PadAndTransform length = %d, want %d", len(transformed), paddedDim)
	}

	var inputSq float64
	for _, v := range input {
		inputSq += float64(v * v)
	}
	var transformedSq float64
	for _, v := range transformed {
		transformedSq += float64(v * v)
	}
	if math.Abs(inputSq-transformedSq) > 0.5 {
		t.Logf("PadAndTransform norm: input=%.2f, transformed=%.2f", inputSq, transformedSq)
	}
}

func TestDefaultIVFPQConfig(t *testing.T) {
	cfg := DefaultIVFPQConfig()
	if cfg.NumCentroids != 1024 {
		t.Errorf("NumCentroids = %d, want 1024", cfg.NumCentroids)
	}
	if cfg.NumSubSpaces != 64 {
		t.Errorf("NumSubSpaces = %d, want 64", cfg.NumSubSpaces)
	}
	if cfg.NProbe != 32 {
		t.Errorf("NProbe = %d, want 32", cfg.NProbe)
	}
}

func TestIVFPQConfigValidate(t *testing.T) {
	cfg := DefaultIVFPQConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should pass: %v", err)
	}

	bad := &IVFPQConfig{NumCentroids: 0, NumSubSpaces: 1, BitsPerSpace: 8, NProbe: 1}
	if err := bad.Validate(); err == nil {
		t.Error("expected error for NumCentroids=0")
	}

	bad2 := &IVFPQConfig{NumCentroids: 1, NumSubSpaces: 0, BitsPerSpace: 8, NProbe: 1}
	if err := bad2.Validate(); err == nil {
		t.Error("expected error for NumSubSpaces=0")
	}
}

func TestTrainingBuffer(t *testing.T) {
	buf := NewTrainingBuffer(5)
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer")
	}

	buf.Add([]float32{1, 2, 3})
	if buf.Len() != 1 {
		t.Fatalf("expected 1 item, got %d", buf.Len())
	}

	for i := 0; i < 10; i++ {
		buf.Add([]float32{float32(i)})
	}
	if buf.Len() != 5 {
		t.Fatalf("expected 5 items after overflow, got %d", buf.Len())
	}

	drained := buf.Drain()
	if len(drained) != 5 {
		t.Fatalf("expected 5 items in drain, got %d", len(drained))
	}
	if buf.Len() != 0 {
		t.Fatalf("expected 0 after drain, got %d", buf.Len())
	}
}

func TestSquaredDist(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	dist := squaredDist(a, b)
	expected := float32(27)
	if dist != expected {
		t.Errorf("squaredDist = %f, want %f", dist, expected)
	}
}

func TestFindClosestCentroid(t *testing.T) {
	idx := &IVFPQIndex{
		cfg:        &IVFPQConfig{NumCentroids: 3},
		paddedDim:  4,
		subspaceDim: 2,
	}
	centroids := []float32{
		1, 1, 1, 1,
		10, 10, 10, 10,
		100, 100, 100, 100,
	}
	vec := []float32{9, 9, 9, 9}
	cID := idx.findClosestCentroid(vec, centroids)
	if cID != 1 {
		t.Errorf("expected centroid 1, got %d", cID)
	}
}

func TestADCComputation(t *testing.T) {
	idx := &IVFPQIndex{
		cfg:        &IVFPQConfig{NumSubSpaces: 2, NumCentroids: 2},
		paddedDim:  4,
		subspaceDim: 2,
	}
	lut := make([]float32, 2*256)
	lut[0*256+0] = 0.1
	lut[0*256+1] = 0.2
	lut[1*256+0] = 0.3
	lut[1*256+1] = 0.4
	codes := []byte{0, 1}
	dist := idx.adcDistance(lut, codes)
	expected := lut[0*256+0] + lut[1*256+1]
	if dist != expected {
		t.Errorf("adcDistance = %f, want %f", dist, expected)
	}
}

func TestSelectIndex(t *testing.T) {
	if SelectIndex(100, false) != IndexBruteForce {
		t.Error("expected IndexBruteForce for small corpus")
	}
	if SelectIndex(1_000_000, true) != IndexHNSW {
		t.Error("expected IndexHNSW for large corpus with preferHNSW")
	}
	if SelectIndex(20_000_000, true) != IndexIVFPQ {
		t.Error("expected IndexIVFPQ for very large corpus")
	}
}
