package datasets

import (
	"math"
	"testing"
)

func TestSIFTSampleRoundTrip(t *testing.T) {
	sample := LoadSIFTSample()
	if len(sample.Vectors) != 1000 {
		t.Fatalf("expected 1000 vectors, got %d", len(sample.Vectors))
	}
	if len(sample.Queries) != 100 {
		t.Fatalf("expected 100 queries, got %d", len(sample.Queries))
	}
	if sample.Dim != 128 {
		t.Fatalf("expected dim 128, got %d", sample.Dim)
	}

	for i, v := range sample.Vectors {
		var sum float32
		for _, val := range v {
			sum += val * val
		}
		norm := float32(math.Sqrt(float64(sum)))
		if norm < 0.99 || norm > 1.01 {
			t.Errorf("vector %d not unit length: norm=%f", i, norm)
		}
	}
}
