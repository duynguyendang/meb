package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type TrainingBuffer struct {
	mu  sync.Mutex
	buf [][]float32
	cap int
}

func NewTrainingBuffer(capacity int) *TrainingBuffer {
	return &TrainingBuffer{
		buf: make([][]float32, 0, capacity),
		cap: capacity,
	}
}

func (tb *TrainingBuffer) Add(vec []float32) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if len(tb.buf) >= tb.cap {
		tb.buf = tb.buf[1:]
	}
	v := make([]float32, len(vec))
	copy(v, vec)
	tb.buf = append(tb.buf, v)
}

func (tb *TrainingBuffer) Drain() [][]float32 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	result := tb.buf
	tb.buf = nil
	return result
}

func (tb *TrainingBuffer) Len() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return len(tb.buf)
}

func (idx *IVFPQIndex) Train(topicID uint32, sampleSize int) error {
	if sampleSize <= 0 {
		sampleSize = idx.cfg.NSample
	}

	samples := idx.trainingBuf.Drain()
	if len(samples) == 0 {
		var err error
		samples, err = idx.sampleVectors(topicID, sampleSize)
		if err != nil {
			return fmt.Errorf("sampling vectors: %w", err)
		}
	}
	if len(samples) == 0 {
		return fmt.Errorf("no vectors found for topic %d", topicID)
	}

	transformed := make([][]float32, len(samples))
	for i, vec := range samples {
		transformed[i] = PadAndTransform(vec, idx.paddedDim)
	}

	centroids := idx.miniBatchKMeans(transformed, idx.cfg.NumCentroids)

	if err := idx.writeCentroids(topicID, centroids); err != nil {
		return fmt.Errorf("writing centroids: %w", err)
	}

	codebook := idx.trainPQCodebook(transformed, centroids)

	if err := idx.writeCodebook(topicID, codebook); err != nil {
		return fmt.Errorf("writing codebook: %w", err)
	}

	return nil
}

func (idx *IVFPQIndex) sampleVectors(topicID uint32, sampleSize int) ([][]float32, error) {
	var samples [][]float32

	err := idx.db.View(func(txn *badger.Txn) error {
		prefix := keys.EncodeIVFRawVectorKey(topicID, 0)[:5]
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true
		opts.PrefetchSize = 1000

		iter := txn.NewIterator(opts)
		defer iter.Close()

		rng := rand.New(rand.NewSource(42))
		count := 0

		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			_ = item.Value(func(val []byte) error {
				if len(val) < idx.fullDim*4 {
					return nil
				}
				vec := make([]float32, idx.fullDim)
				for i := 0; i < idx.fullDim; i++ {
					vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(val[i*4:]))
				}
				count++
				if len(samples) < sampleSize {
					samples = append(samples, vec)
				} else {
					j := rng.Intn(count)
					if j < sampleSize {
						samples[j] = vec
					}
				}
				return nil
			})
		}
		return nil
	})
	return samples, err
}

func (idx *IVFPQIndex) miniBatchKMeans(samples [][]float32, numCentroids int) []float32 {
	if len(samples) == 0 {
		return nil
	}

	rng := rand.New(rand.NewSource(42))
	dim := idx.paddedDim

	centroids := make([]float32, numCentroids*dim)

	firstIdx := rng.Intn(len(samples))
	copy(centroids[0*dim:1*dim], samples[firstIdx])

	for c := 1; c < numCentroids; c++ {
		dists := make([]float64, len(samples))
		sum := 0.0
		for i, s := range samples {
			minDist := math.MaxFloat64
			for j := 0; j < c; j++ {
				d := float64(squaredDist(s, centroids[j*dim:(j+1)*dim]))
				if d < minDist {
					minDist = d
				}
			}
			dists[i] = minDist
			sum += minDist
		}

		threshold := rng.Float64() * sum
		cum := 0.0
		for i, d := range dists {
			cum += d
			if cum >= threshold {
				copy(centroids[c*dim:(c+1)*dim], samples[i])
				break
			}
		}
	}

	batchSize := idx.cfg.BatchSize
	if batchSize > len(samples) {
		batchSize = len(samples)
	}

	for iter := 0; iter < 100; iter++ {
		batch := make([][]float32, batchSize)
		for i := range batch {
			batch[i] = samples[rng.Intn(len(samples))]
		}

		counts := make([]int, numCentroids)
		for _, s := range batch {
			bestC := 0
			bestDist := float32(math.MaxFloat32)
			for c := 0; c < numCentroids; c++ {
				d := squaredDist(s, centroids[c*dim:(c+1)*dim])
				if d < bestDist {
					bestDist = d
					bestC = c
				}
			}
			counts[bestC]++
			learningRate := 1.0 / float32(counts[bestC])
			for d := 0; d < dim; d++ {
				centroids[bestC*dim+d] += learningRate * (s[d] - centroids[bestC*dim+d])
			}
		}
	}

	return centroids
}

func (idx *IVFPQIndex) trainPQCodebook(samples [][]float32, centroids []float32) []float32 {
	numCentroids := idx.cfg.NumCentroids
	numSubSpaces := idx.cfg.NumSubSpaces
	subspaceDim := idx.subspaceDim

	codebook := make([]float32, numSubSpaces*256*subspaceDim)

	residuals := make([][]float32, len(samples))
	for i, s := range samples {
		bestC := 0
		bestDist := float32(math.MaxFloat32)
		for c := 0; c < numCentroids; c++ {
			d := squaredDist(s, centroids[c*idx.paddedDim:(c+1)*idx.paddedDim])
			if d < bestDist {
				bestDist = d
				bestC = c
			}
		}
		residual := make([]float32, idx.paddedDim)
		for d := 0; d < idx.paddedDim; d++ {
			residual[d] = s[d] - centroids[bestC*idx.paddedDim+d]
		}
		residuals[i] = residual
	}

	for s := 0; s < numSubSpaces; s++ {
		subspaceVecs := make([][]float32, len(residuals))
		for i, res := range residuals {
			subspaceVecs[i] = res[s*subspaceDim : (s+1)*subspaceDim]
		}

		rng := rand.New(rand.NewSource(42 + int64(s)))

		subCentroids := make([]float32, 256*subspaceDim)

		firstIdx := rng.Intn(len(subspaceVecs))
		copy(subCentroids[0*subspaceDim:1*subspaceDim], subspaceVecs[firstIdx])

		for c := 1; c < 256; c++ {
			dists := make([]float64, len(subspaceVecs))
			sum := 0.0
			for i, sv := range subspaceVecs {
				minDist := math.MaxFloat64
				for j := 0; j < c; j++ {
					d := float64(squaredDist(sv, subCentroids[j*subspaceDim:(j+1)*subspaceDim]))
					if d < minDist {
						minDist = d
					}
				}
				dists[i] = minDist
				sum += minDist
			}

			threshold := rng.Float64() * sum
			cum := 0.0
			for i, d := range dists {
				cum += d
				if cum >= threshold {
					copy(subCentroids[c*subspaceDim:(c+1)*subspaceDim], subspaceVecs[i])
					break
				}
			}
		}

		batchSize := 1000
		if batchSize > len(subspaceVecs) {
			batchSize = len(subspaceVecs)
		}

		counts := make([]int, 256)
		for iter := 0; iter < 50; iter++ {
			batch := make([][]float32, batchSize)
			for i := range batch {
				batch[i] = subspaceVecs[rng.Intn(len(subspaceVecs))]
			}

			for _, sv := range batch {
				bestC := 0
				bestDist := float32(math.MaxFloat32)
				for c := 0; c < 256; c++ {
					d := squaredDist(sv, subCentroids[c*subspaceDim:(c+1)*subspaceDim])
					if d < bestDist {
						bestDist = d
						bestC = c
					}
				}
				counts[bestC]++
				lr := 1.0 / float32(counts[bestC])
				for d := 0; d < subspaceDim; d++ {
					subCentroids[bestC*subspaceDim+d] += lr * (sv[d] - subCentroids[bestC*subspaceDim+d])
				}
			}
		}

		for c := 0; c < 256; c++ {
			for d := 0; d < subspaceDim; d++ {
				codebook[s*256*subspaceDim+c*subspaceDim+d] = subCentroids[c*subspaceDim+d]
			}
		}
	}

	return codebook
}

func (idx *IVFPQIndex) writeCentroids(topicID uint32, centroids []float32) error {
	return idx.db.Update(func(txn *badger.Txn) error {
		buf := make([]byte, idx.paddedDim*4)
		for c := 0; c < idx.cfg.NumCentroids; c++ {
			key := keys.EncodeIVFCentroidKey(topicID, uint32(c))
			for d := 0; d < idx.paddedDim; d++ {
				binary.LittleEndian.PutUint32(buf[d*4:], math.Float32bits(centroids[c*idx.paddedDim+d]))
			}
			if err := txn.Set(key, buf); err != nil {
				return err
			}
		}
		return nil
	})
}

func (idx *IVFPQIndex) writeCodebook(topicID uint32, codebook []float32) error {
	codebookKey := make([]byte, 5)
	codebookKey[0] = keys.IVFCodebookPrefix
	binary.BigEndian.PutUint32(codebookKey[1:5], topicID)

	buf := make([]byte, len(codebook)*4)
	for i, v := range codebook {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}

	return idx.db.Update(func(txn *badger.Txn) error {
		return txn.Set(codebookKey, buf)
	})
}

func squaredDist(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}
