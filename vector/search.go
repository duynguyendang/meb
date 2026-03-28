package vector

import (
	"container/heap"
	"log/slog"
	"sort"
)

type SearchResult struct {
	ID    uint64
	Score float32
}

type scoreIndex struct {
	score float32
	idx   int
}

type scoreHeap []scoreIndex

func (h scoreHeap) Len() int           { return len(h) }
func (h scoreHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h scoreHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scoreHeap) Push(x any)        { *h = append(*h, x.(scoreIndex)) }
func (h *scoreHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Search performs a parallel linear scan to find the top-k most similar vectors.
// Uses TurboQuant blockwise dot product on full 1536-dim compressed vectors.
func (r *VectorRegistry) Search(queryVec []float32, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	slog.Debug("TQ vector search started",
		"vectorCount", r.Count(),
		"k", k,
		"numWorkers", r.config.NumWorkers,
	)

	// Compress query to TQ format
	tqQuery := QuantizeTurboQuant(queryVec, r.tqConfig)

	r.mu.RLock()
	numVectors := len(r.revMap)
	if numVectors == 0 {
		r.mu.RUnlock()
		return nil, nil
	}

	data := r.data
	vectorSize := r.vectorSize
	r.mu.RUnlock()

	numWorkers := r.config.NumWorkers
	if numWorkers > numVectors {
		numWorkers = numVectors
	}

	vectorsPerWorker := (numVectors + numWorkers - 1) / numWorkers

	resultCh := make(chan []SearchResult, numWorkers)
	actualWorkers := 0

	for i := 0; i < numWorkers; i++ {
		startIdx := i * vectorsPerWorker
		endIdx := startIdx + vectorsPerWorker
		if endIdx > numVectors {
			endIdx = numVectors
		}
		if startIdx >= endIdx {
			break
		}

		actualWorkers++
		go func(start, end int) {
			topK := r.scanChunkTQ(data, tqQuery, start, end, k, vectorSize)
			resultCh <- topK
		}(startIdx, endIdx)
	}

	allResults := make([]SearchResult, 0, k*actualWorkers)
	for i := 0; i < actualWorkers; i++ {
		results := <-resultCh
		allResults = append(allResults, results...)
	}
	close(resultCh)

	return getTopK(allResults, k), nil
}

// scanChunkTQ scans a chunk of TQ vectors and returns local top-k results.
func (r *VectorRegistry) scanChunkTQ(data []byte, query []byte, startIdx, endIdx, k int, vectorSize int) []SearchResult {
	h := make(scoreHeap, 0, k)
	dim := r.config.FullDim
	tqCfg := r.tqConfig

	for idx := startIdx; idx < endIdx; idx++ {
		offset := idx * vectorSize
		vecData := data[offset : offset+vectorSize]
		score := DotProductTurboQuant(vecData, query, dim, tqCfg)

		if len(h) < k {
			h = append(h, scoreIndex{score: score, idx: idx})
			if len(h) == k {
				heap.Init(&h)
			}
		} else if score > h[0].score {
			h[0] = scoreIndex{score: score, idx: idx}
			heap.Fix(&h, 0)
		}
	}

	results := make([]SearchResult, 0, len(h))
	r.mu.RLock()
	for _, si := range h {
		if si.idx < len(r.revMap) {
			results = append(results, SearchResult{
				ID:    r.revMap[si.idx],
				Score: si.score,
			})
		}
	}
	r.mu.RUnlock()

	return results
}

func getTopK(results []SearchResult, k int) []SearchResult {
	if len(results) == 0 {
		return results
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > k {
		return results[:k]
	}
	return results
}
