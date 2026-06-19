package datasets

import (
	"math"
	"math/rand"
)

// SIFTSample holds a small embedded SIFT-128 dataset for testing.
// Full SIFT-1M is not committed to git; download via Makefile target.
type SIFTSample struct {
	Vectors [][]float32
	Queries [][]float32
	Dim     int
}

// LoadSIFTSample generates a 1K-vector 128-dim SIFT-like sample for testing.
func LoadSIFTSample() *SIFTSample {
	rng := rand.New(rand.NewSource(42))
	nVecs := 1000
	nQueries := 100
	dim := 128

	vecs := make([][]float32, nVecs)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		var sum float32
		for _, val := range v {
			sum += val * val
		}
		norm := float32(math.Sqrt(float64(sum)))
		if norm > 0 {
			for j := range v {
				v[j] /= norm
			}
		}
		vecs[i] = v
	}

	queries := make([][]float32, nQueries)
	for i := range queries {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		var sum float32
		for _, val := range v {
			sum += val * val
		}
		norm := float32(math.Sqrt(float64(sum)))
		if norm > 0 {
			for j := range v {
				v[j] /= norm
			}
		}
		queries[i] = v
	}

	return &SIFTSample{
		Vectors: vecs,
		Queries: queries,
		Dim:     dim,
	}
}
