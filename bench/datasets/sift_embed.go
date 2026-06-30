package datasets

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
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

// SIFT1MDataset holds the full SIFT1M dataset with ground truth.
type SIFT1MDataset struct {
	Base       [][]float32 // 1M base vectors
	Queries    [][]float32 // 10K query vectors
	GroundTruth [][]uint32 // 10K x 1000 ground truth neighbors
	Dim        int
}

// LoadSIFT1M attempts to load the full SIFT1M dataset from the standard location.
// Returns nil if the dataset is not available (not downloaded).
// Expected files in dataDir:
//   - sift_base.fvecs: 1M base vectors (128-dim)
//   - sift_query.fvecs: 10K query vectors (128-dim)
//   - sift_groundtruth.ivecs: 10K x 1000 ground truth neighbors
func LoadSIFT1M(dataDir string) *SIFT1MDataset {
	basePath := filepath.Join(dataDir, "sift_base.fvecs")
	queryPath := filepath.Join(dataDir, "sift_query.fvecs")
	gtPath := filepath.Join(dataDir, "sift_groundtruth.ivecs")

	// Check if files exist
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil
	}
	if _, err := os.Stat(queryPath); os.IsNotExist(err) {
		return nil
	}
	if _, err := os.Stat(gtPath); os.IsNotExist(err) {
		return nil
	}

	// Load base vectors
	base, dim, err := loadFvecs(basePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to load SIFT1M base: %v\n", err)
		return nil
	}

	// Load query vectors
	queries, _, err := loadFvecs(queryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to load SIFT1M queries: %v\n", err)
		return nil
	}

	// Load ground truth
	gt, err := loadIvecs(gtPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to load SIFT1M ground truth: %v\n", err)
		return nil
	}

	return &SIFT1MDataset{
		Base:        base,
		Queries:     queries,
		GroundTruth: gt,
		Dim:         dim,
	}
}

// loadFvecs loads vectors from a .fvecs file (float32 vectors).
// Format: [dim: int32][vec: float32[dim]] repeated
func loadFvecs(path string) ([][]float32, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var vectors [][]float32
	var dim int

	for {
		// Read dimension
		var d int32
		if err := binary.Read(reader, binary.LittleEndian, &d); err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}

		if dim == 0 {
			dim = int(d)
		} else if int(d) != dim {
			return nil, 0, fmt.Errorf("inconsistent dimension: expected %d, got %d", dim, d)
		}

		// Read vector
		vec := make([]float32, dim)
		for i := 0; i < dim; i++ {
			if err := binary.Read(reader, binary.LittleEndian, &vec[i]); err != nil {
				return nil, 0, err
			}
		}
		vectors = append(vectors, vec)
	}

	return vectors, dim, nil
}

// loadIvecs loads integer vectors from an .ivecs file (int32 vectors).
// Format: [dim: int32][vec: int32[dim]] repeated
func loadIvecs(path string) ([][]uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var vectors [][]uint32

	for {
		// Read dimension
		var d int32
		if err := binary.Read(reader, binary.LittleEndian, &d); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read vector
		vec := make([]uint32, d)
		for i := int32(0); i < d; i++ {
			var v int32
			if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			vec[i] = uint32(v)
		}
		vectors = append(vectors, vec)
	}

	return vectors, nil
}

