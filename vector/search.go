package vector

import (
	"container/heap"
	"iter"
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

const TopicIDMask = uint64(0xFFFFFF) << 40

// Search returns an iterator of top-k most similar vectors.
func (r *VectorRegistry) Search(queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return r.SearchWithFilter(queryVec, k, 0)
}

// SearchWithFilter returns an iterator of top-k most similar vectors matching a semantic hash.
func (r *VectorRegistry) SearchWithFilter(queryVec []float32, k int, filterHash uint8) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		if k <= 0 {
			return
		}

		slog.Debug("TQ vector search started",
			"vectorCount", r.Count(),
			"k", k,
			"numWorkers", r.config.NumWorkers,
		)

		hybridQuery := QuantizeHybrid(queryVec, r.hybridCfg)

		r.mu.RLock()
		numVectors := r.totalVectors
		if numVectors == 0 {
			r.mu.RUnlock()
			return
		}
		vectorSize := r.vectorSize
		revMap := make([]uint64, len(r.revMap))
		copy(revMap, r.revMap)
		// Capture function to access vector slots (safe: segments won't shrink)
		getSlot := func(idx int) []byte { return r.getVectorSlice(idx) }
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
				defer func() { recover() }()
				topK := scanChunkHybrid(getSlot, hybridQuery, start, end, k, vectorSize, revMap, filterHash, r.config.FullDim, r.hybridCfg)
				resultCh <- topK
			}(startIdx, endIdx)
		}

		allResults := make([]SearchResult, 0, k*actualWorkers)
		for i := 0; i < actualWorkers; i++ {
			results := <-resultCh
			allResults = append(allResults, results...)
		}
		close(resultCh)

		final := getTopK(allResults, k)
		for _, sr := range final {
			if !yield(sr, nil) {
				return
			}
		}
	}
}

// SearchInTopic returns an iterator of top-k most similar vectors within a specific topic.
func (r *VectorRegistry) SearchInTopic(topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		if k <= 0 {
			return
		}

		slog.Debug("TQ topic-aware vector search started",
			"topicID", topicID,
			"vectorCount", r.Count(),
			"k", k,
		)

		hybridQuery := QuantizeHybrid(queryVec, r.hybridCfg)

		r.mu.RLock()
		numVectors := r.totalVectors
		if numVectors == 0 {
			r.mu.RUnlock()
			return
		}
		vectorSize := r.vectorSize
		revMap := make([]uint64, len(r.revMap))
		copy(revMap, r.revMap)
		getSlot := func(idx int) []byte { return r.getVectorSlice(idx) }
		r.mu.RUnlock()

		topicFilter := uint64(topicID) << 40

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
				defer func() { recover() }()
				topK := scanChunkHybridWithTopic(getSlot, hybridQuery, start, end, k, vectorSize, revMap, topicFilter, r.config.FullDim, r.hybridCfg)
				resultCh <- topK
			}(startIdx, endIdx)
		}

		allResults := make([]SearchResult, 0, k*actualWorkers)
		for i := 0; i < actualWorkers; i++ {
			results := <-resultCh
			allResults = append(allResults, results...)
		}
		close(resultCh)

		final := getTopK(allResults, k)
		for _, sr := range final {
			if !yield(sr, nil) {
				return
			}
		}
	}
}

// scanChunkHybrid scans a chunk of Hybrid vectors using a slot accessor function.
func scanChunkHybrid(getSlot func(int) []byte, query []byte, startIdx, endIdx, k int, vectorSize int, revMap []uint64, filterHash uint8, fullDim int, hybridCfg *HybridConfig) []SearchResult {
	h := make(scoreHeap, 0, k)

	for idx := startIdx; idx < endIdx; idx++ {
		slot := getSlot(idx)

		hashByte := slot[0]
		if filterHash != 0 && hashByte != filterHash {
			continue
		}

		vecData := slot[hashSize:vectorSize]
		score := DotProductHybrid(vecData, query, fullDim, hybridCfg)

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
	for _, si := range h {
		if si.idx < len(revMap) {
			results = append(results, SearchResult{
				ID:    revMap[si.idx],
				Score: si.score,
			})
		}
	}

	return results
}

// scanChunkHybridWithTopic scans a chunk of Hybrid vectors filtering by topic ID.
func scanChunkHybridWithTopic(getSlot func(int) []byte, query []byte, startIdx, endIdx, k int, vectorSize int, revMap []uint64, topicFilter uint64, fullDim int, hybridCfg *HybridConfig) []SearchResult {
	h := make(scoreHeap, 0, k)
	topicMask := TopicIDMask

	for idx := startIdx; idx < endIdx; idx++ {
		if idx >= len(revMap) {
			break
		}
		if (revMap[idx] & topicMask) != topicFilter {
			continue
		}

		slot := getSlot(idx)
		vecData := slot[hashSize:vectorSize]
		score := DotProductHybrid(vecData, query, fullDim, hybridCfg)

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
	for _, si := range h {
		if si.idx < len(revMap) {
			results = append(results, SearchResult{
				ID:    revMap[si.idx],
				Score: si.score,
			})
		}
	}

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

// DecodeVectorTopic extracts the TopicID from a vector's packed ID in the registry.
func DecodeVectorTopic(revMap []uint64, idx int) uint32 {
	if idx < 0 || idx >= len(revMap) {
		return 0
	}
	return uint32((revMap[idx] & TopicIDMask) >> 40)
}
