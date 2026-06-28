package vector

import (
	"context"
	"encoding/binary"
	"iter"
	"math"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type IVFPQIndex struct {
	db         *badger.DB
	cfg        *IVFPQConfig
	fullDim    int
	paddedDim  int
	subspaceDim int

	centroidsMu sync.RWMutex
	centroids   map[uint32][]float32

	codebookMu sync.RWMutex
	codebook   map[uint32][]float32

	trainingBuf *TrainingBuffer
}

func NewIVFPQIndex(db *badger.DB, cfg *IVFPQConfig, fullDim int) *IVFPQIndex {
	paddedDim := nextPow2(fullDim)
	subspaceDim := paddedDim / cfg.NumSubSpaces
	return &IVFPQIndex{
		db:          db,
		cfg:         cfg,
		fullDim:     fullDim,
		paddedDim:   paddedDim,
		subspaceDim: subspaceDim,
		centroids:   make(map[uint32][]float32),
		codebook:    make(map[uint32][]float32),
		trainingBuf: NewTrainingBuffer(cfg.NSample),
	}
}

func (idx *IVFPQIndex) AddVector(txn *badger.Txn, topicID uint32, localID uint64, fullVec []float32) error {
	transformed := PadAndTransform(fullVec, idx.paddedDim)

	idx.trainingBuf.Add(fullVec)

	// Store raw float32 vector for future training
	rawKey := keys.EncodeIVFRawVectorKey(topicID, localID)
	rawBuf := make([]byte, idx.fullDim*4)
	for i := 0; i < idx.fullDim; i++ {
		binary.LittleEndian.PutUint32(rawBuf[i*4:], math.Float32bits(fullVec[i]))
	}
	if err := txn.Set(rawKey, rawBuf); err != nil {
		return err
	}

	centroids, err := idx.loadCentroids(txn, topicID)
	if err != nil {
		return err
	}

	if centroids == nil {
		// No centroids yet — training has not been run.
		// Just store the raw vector, skip IVF-PQ encoding.
		return nil
	}

	centroidID := idx.findClosestCentroid(transformed, centroids)

	residual := make([]float32, idx.paddedDim)
	centroidBase := int(centroidID) * idx.paddedDim
	for i := 0; i < idx.paddedDim; i++ {
		residual[i] = transformed[i] - centroids[centroidBase+i]
	}

	codebook, err := idx.loadCodebook(txn, topicID)
	if err != nil {
		return err
	}
	if codebook == nil {
		return nil
	}
	pqCodes := idx.quantizeResidual(residual, codebook)

	postingKey := keys.EncodeIVFPostingKey(topicID, centroidID, localID)
	return txn.Set(postingKey, pqCodes)
}

func (idx *IVFPQIndex) loadCentroids(txn *badger.Txn, topicID uint32) ([]float32, error) {
	idx.centroidsMu.RLock()
	if c, ok := idx.centroids[topicID]; ok {
		idx.centroidsMu.RUnlock()
		return c, nil
	}
	idx.centroidsMu.RUnlock()

	centroidSize := idx.cfg.NumCentroids * idx.paddedDim
	centroids := make([]float32, centroidSize)

	prefix := keys.EncodeIVFCentroidKey(topicID, 0)[:5]
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	opts.PrefetchSize = idx.cfg.NumCentroids

	iter := txn.NewIterator(opts)
	defer iter.Close()

	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		cID := keys.ExtractCentroidIDFromPostingKey(key)
		err := item.Value(func(val []byte) error {
			base := int(cID) * idx.paddedDim
			for i := 0; i < idx.paddedDim && i*4 < len(val); i++ {
				centroids[base+i] = math.Float32frombits(binary.LittleEndian.Uint32(val[i*4:]))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		count++
	}

	if count == 0 {
		return nil, nil
	}

	idx.centroidsMu.Lock()
	idx.centroids[topicID] = centroids
	idx.centroidsMu.Unlock()

	return centroids, nil
}

func (idx *IVFPQIndex) findClosestCentroid(vec []float32, centroids []float32) uint32 {
	numCentroids := idx.cfg.NumCentroids
	bestIdx := uint32(0)
	bestDist := float32(math.MaxFloat32)

	for c := 0; c < numCentroids; c++ {
		base := c * idx.paddedDim
		dist := float32(0)
		for i := 0; i < idx.paddedDim; i++ {
			diff := vec[i] - centroids[base+i]
			dist += diff * diff
		}
		if dist < bestDist {
			bestDist = dist
			bestIdx = uint32(c)
		}
	}
	return bestIdx
}

func (idx *IVFPQIndex) quantizeResidual(residual []float32, codebook []float32) []byte {
	codes := make([]byte, idx.cfg.NumSubSpaces)

	for s := 0; s < idx.cfg.NumSubSpaces; s++ {
		base := s * idx.subspaceDim
		bestCode := byte(0)
		bestDist := float32(math.MaxFloat32)

		for code := 0; code < 256; code++ {
			cbOffset := s*256*idx.subspaceDim + code*idx.subspaceDim
			dist := float32(0)
			for d := 0; d < idx.subspaceDim; d++ {
				diff := residual[base+d] - codebook[cbOffset+d]
				dist += diff * diff
			}
			if dist < bestDist {
				bestDist = dist
				bestCode = byte(code)
			}
		}
		codes[s] = bestCode
	}
	return codes
}

func (idx *IVFPQIndex) SearchInTopic(ctx context.Context, txn *badger.Txn, topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		transformed := PadAndTransform(queryVec, idx.paddedDim)

		centroids, err := idx.loadCentroids(txn, topicID)
		if err != nil {
			yield(SearchResult{}, err)
			return
		}
		if centroids == nil {
			return
		}

		topCentroids := idx.findTopNCentroids(transformed, centroids)

		codebook, err := idx.loadCodebook(txn, topicID)
		if err != nil {
			yield(SearchResult{}, err)
			return
		}
		if codebook == nil {
			return
		}

		lut := idx.precomputeLUT(transformed, codebook)

		heap := newMinHeap(k)
		for _, centroidID := range topCentroids {
			if err := ctx.Err(); err != nil {
				yield(SearchResult{}, err)
				return
			}

			prefix := keys.IVFPostingPrefixByCentroid(topicID, centroidID)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = true
			opts.PrefetchSize = 1000

			iter := txn.NewIterator(opts)
			for iter.Rewind(); iter.Valid(); iter.Next() {
				item := iter.Item()
				localID := keys.ExtractLocalIDFromPostingKey(item.Key())

				var pqCodes []byte
				_ = item.Value(func(val []byte) error {
					pqCodes = make([]byte, len(val))
					copy(pqCodes, val)
					return nil
				})

				dist := idx.adcDistance(lut, pqCodes)

				heap.push(SearchResult{
					ID:    keys.PackID(topicID, localID),
					Score: 1.0 / (1.0 + dist),
				})
				if heap.Len() > k {
					heap.pop()
				}
			}
			iter.Close()
		}

		results := heap.drain()
		for i := len(results) - 1; i >= 0; i-- {
			if !yield(results[i], nil) {
				return
			}
		}
	}
}

func (idx *IVFPQIndex) findTopNCentroids(queryVec []float32, centroids []float32) []uint32 {
	type centroidDist struct {
		id   uint32
		dist float32
	}
	nprobe := idx.cfg.NProbe
	if nprobe > idx.cfg.NumCentroids {
		nprobe = idx.cfg.NumCentroids
	}

	dists := make([]centroidDist, idx.cfg.NumCentroids)
	for c := 0; c < idx.cfg.NumCentroids; c++ {
		base := c * idx.paddedDim
		dist := float32(0)
		for i := 0; i < idx.paddedDim; i++ {
			diff := queryVec[i] - centroids[base+i]
			dist += diff * diff
		}
		dists[c] = centroidDist{id: uint32(c), dist: dist}
	}

	sort.Slice(dists, func(i, j int) bool {
		return dists[i].dist < dists[j].dist
	})

	result := make([]uint32, nprobe)
	for i := 0; i < nprobe; i++ {
		result[i] = dists[i].id
	}
	return result
}

func (idx *IVFPQIndex) precomputeLUT(transformed []float32, codebook []float32) []float32 {
	lut := make([]float32, idx.cfg.NumSubSpaces*256)
	for s := 0; s < idx.cfg.NumSubSpaces; s++ {
		base := s * idx.subspaceDim
		for code := 0; code < 256; code++ {
			cbOffset := s*256*idx.subspaceDim + code*idx.subspaceDim
			dist := float32(0)
			for d := 0; d < idx.subspaceDim; d++ {
				diff := transformed[base+d] - codebook[cbOffset+d]
				dist += diff * diff
			}
			lut[s*256+code] = dist
		}
	}
	return lut
}

func (idx *IVFPQIndex) adcDistance(lut []float32, pqCodes []byte) float32 {
	dist := float32(0)
	for s := 0; s < len(pqCodes); s++ {
		dist += lut[s*256+int(pqCodes[s])]
	}
	return dist
}

func (idx *IVFPQIndex) loadCodebook(txn *badger.Txn, topicID uint32) ([]float32, error) {
	idx.codebookMu.RLock()
	if cb, ok := idx.codebook[topicID]; ok {
		idx.codebookMu.RUnlock()
		return cb, nil
	}
	idx.codebookMu.RUnlock()

	codebookKey := make([]byte, 5)
	codebookKey[0] = keys.IVFCodebookPrefix
	binary.BigEndian.PutUint32(codebookKey[1:5], topicID)

	var codebook []float32
	item, err := txn.Get(codebookKey)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	err = item.Value(func(val []byte) error {
		codebook = make([]float32, len(val)/4)
		for i := range codebook {
			codebook[i] = math.Float32frombits(binary.LittleEndian.Uint32(val[i*4:]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	idx.codebookMu.Lock()
	idx.codebook[topicID] = codebook
	idx.codebookMu.Unlock()

	return codebook, nil
}

// LoadCentroidsForDebug loads centroids for debugging purposes.
func (idx *IVFPQIndex) LoadCentroidsForDebug(txn *badger.Txn, topicID uint32) ([]float32, error) {
	return idx.loadCentroids(txn, topicID)
}


