package vector

import (
	"log/slog"
	"runtime"
)

// SearchResult represents a single search result with similarity score.
type SearchResult struct {
	ID    uint64  // GraphID
	Score float32 // Cosine similarity (higher is better)
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
	type scoreIndex struct {
		score float32
		idx   int
	}

	// Simple heap implementation for top-k
	topK := make([]scoreIndex, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product
		offset := idx * MRLDim
		score := DotProduct(query, vectors[offset:offset+MRLDim])

		// Maintain top-k
		if len(topK) < k {
			topK = append(topK, scoreIndex{score: score, idx: idx})
			// Bubble up to maintain order
			for i := len(topK) - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		} else if score > topK[k-1].score {
			topK[k-1] = scoreIndex{score: score, idx: idx}
			// Bubble up
			for i := k - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		}
	}

	// Convert to SearchResult
	results := make([]SearchResult, 0, len(topK))
	r.mu.RLock()
	for _, si := range topK {
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
	type scoreIndex struct {
		score float32
		idx   int
	}

	// Pre-allocate top-K array to avoid allocations during scan
	topK := make([]scoreIndex, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product using SIMD-optimized function
		// Zero-allocation: just pointer arithmetic into the flat buffer
		offset := idx * MRLDim
		score := DotProduct64(data[offset:offset+MRLDim], query)

		// Maintain top-k using simple bubble-up (fast for small k)
		if len(topK) < k {
			topK = append(topK, scoreIndex{score: score, idx: idx})
			// Bubble up to maintain order
			for i := len(topK) - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		} else if score > topK[k-1].score {
			topK[k-1] = scoreIndex{score: score, idx: idx}
			// Bubble up
			for i := k - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		}
	}

	// Convert to SearchResult (only allocate for final results)
	results := make([]SearchResult, 0, len(topK))
	r.mu.RLock()
	for _, si := range topK {
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
	type scoreIndex struct {
		score float32
		idx   int
	}

	// Pre-allocate top-K array to avoid allocations during scan
	topK := make([]scoreIndex, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		// Calculate dot product using int8 SIMD-optimized function
		// Zero-allocation: just pointer arithmetic into the flat buffer
		offset := idx * MRLDim
		score := DotProductInt8(data[offset:offset+MRLDim], query)

		// Maintain top-k using simple bubble-up (fast for small k)
		if len(topK) < k {
			topK = append(topK, scoreIndex{score: score, idx: idx})
			// Bubble up to maintain order
			for i := len(topK) - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		} else if score > topK[k-1].score {
			topK[k-1] = scoreIndex{score: score, idx: idx}
			// Bubble up
			for i := k - 1; i > 0; i-- {
				if topK[i].score > topK[i-1].score {
					topK[i], topK[i-1] = topK[i-1], topK[i]
				} else {
					break
				}
			}
		}
	}

	// Convert to SearchResult (only allocate for final results)
	results := make([]SearchResult, 0, len(topK))
	r.mu.RLock()
	for _, si := range topK {
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
	if len(results) <= k {
		return results
	}

	// Simple bubble sort by score descending (highest first)
	// Only need to sort the first k positions
	for i := 0; i < k; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results[:k]
}
