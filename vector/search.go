package vector

import (
	"container/heap"
	"log/slog"
	"runtime"
	"sort"
)

// SearchResult represents a single search result with similarity score.
type SearchResult struct {
	ID    uint64  // GraphID
	Score float32 // Cosine similarity (higher is better)
}

type scoreIndex struct {
	score float32
	idx   int
}

// scoreHeap implements heap.Interface for a min-heap of scoreIndex
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
// Optimized with int8 scalar quantization and SIMD instructions.
func (r *VectorRegistry) Search(queryVec []float32, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	slog.Debug("vector search started",
		"vectorCount", r.Count(),
		"k", k,
		"numCPUs", runtime.NumCPU(),
	)

	// Process query to get 64-d MRL vector
	mrlQuery := ProcessMRL(queryVec)

	// Quantize query for int8 dot product
	quantizedQuery := Quantize(mrlQuery)

	r.mu.RLock()

	numVectors := len(r.revMap)
	if numVectors == 0 {
		r.mu.RUnlock()
		return nil, nil
	}

	// Capture the data slice for parallel processing (copy slice header only - cheap)
	data := r.data

	r.mu.RUnlock()

	// Determine number of workers
	numWorkers := runtime.NumCPU()
	if numWorkers > numVectors {
		numWorkers = numVectors
	}

	// Calculate chunk size
	vectorsPerWorker := (numVectors + numWorkers - 1) / numWorkers

	// Channel to collect results from workers
	resultCh := make(chan []SearchResult, numWorkers)

	// Launch workers and track actual count
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
			topK := r.scanChunkInt8(data, quantizedQuery, start, end, k)
			resultCh <- topK
		}(startIdx, endIdx)
	}

	// Collect and merge results
	allResults := make([]SearchResult, 0, k*actualWorkers)
	for i := 0; i < actualWorkers; i++ {
		results := <-resultCh
		allResults = append(allResults, results...)
	}
	close(resultCh)

	// Get global top-k
	return getTopK(allResults, k), nil
}

// scanChunk scans a chunk of vectors and returns local top-k results.
func (r *VectorRegistry) scanChunk(vectors []float32, query []float32, startIdx, endIdx, k int) []SearchResult {
	h := make(scoreHeap, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product
		offset := idx * MRLDim
		score := DotProduct(query, vectors[offset:offset+MRLDim])

		// Maintain top-k
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

	// Convert to SearchResult
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

// scanChunkSIMD scans a chunk of vectors using SIMD-optimized dot product.
// Zero-allocation per vector scanned (except for the final results).
func (r *VectorRegistry) scanChunkSIMD(data []float32, query []float32, startIdx, endIdx, k int) []SearchResult {
	h := make(scoreHeap, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product using SIMD-optimized function
		// Zero-allocation: just pointer arithmetic into the flat buffer
		offset := idx * MRLDim
		score := DotProduct64(data[offset:offset+MRLDim], query)

		// Maintain top-k
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

	// Convert to SearchResult (only allocate for final results)
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

// scanChunkInt8 scans a chunk of vectors using int8 scalar quantization.
// Provides ~3x speedup over float32 due to better cache efficiency and faster integer arithmetic.
func (r *VectorRegistry) scanChunkInt8(data []int8, query []int8, startIdx, endIdx, k int) []SearchResult {
	h := make(scoreHeap, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product using int8 SIMD-optimized function
		// Zero-allocation: just pointer arithmetic into the flat buffer
		offset := idx * MRLDim
		score := DotProductInt8(data[offset:offset+MRLDim], query)

		// Maintain top-k
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

	// Convert to SearchResult (only allocate for final results)
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

// getTopK extracts the top-k results from a merged list.
func getTopK(results []SearchResult, k int) []SearchResult {
	if len(results) == 0 {
		return results
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score // descending
	})

	if len(results) > k {
		return results[:k]
	}
	return results
}
