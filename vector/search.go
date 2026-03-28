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
// Uses TurboQuant blockwise dot product on full 1536-dim compressed vectors.
// Results are streamed via iter.Seq2 — no intermediate slice allocation.
func (r *VectorRegistry) Search(queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		if k <= 0 {
			return
		}

		slog.Debug("TQ vector search started",
			"vectorCount", r.Count(),
			"k", k,
			"numWorkers", r.config.NumWorkers,
		)

		tqQuery := QuantizeTurboQuant(queryVec, r.tqConfig)

		r.mu.RLock()
		numVectors := len(r.revMap)
		if numVectors == 0 {
			r.mu.RUnlock()
			return
		}

		data := r.data
		vectorSize := r.vectorSize
		revMap := r.revMap
		r.mu.RUnlock()

		numWorkers := r.config.NumWorkers
		if numWorkers > numVectors {
			numWorkers = numVectors
		}

		vectorsPerWorker := (numVectors + numWorkers - 1) / numWorkers

		resultCh := make(chan []SearchResult, numWorkers)
		done := make(chan struct{})
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
				topK := r.scanChunkTQ(data, tqQuery, start, end, k, vectorSize, revMap)
				select {
				case resultCh <- topK:
				case <-done:
				}
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
// Only vectors whose packed ID matches the requested TopicID are scanned.
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

		tqQuery := QuantizeTurboQuant(queryVec, r.tqConfig)

		r.mu.RLock()
		numVectors := len(r.revMap)
		if numVectors == 0 {
			r.mu.RUnlock()
			return
		}

		data := r.data
		vectorSize := r.vectorSize
		revMap := r.revMap
		r.mu.RUnlock()

		packedTopicPrefix := uint64(topicID) << 40
		topicMask := TopicIDMask

		startIdx := -1
		endIdx := -1
		for i, id := range revMap {
			if (id & topicMask) == packedTopicPrefix {
				if startIdx == -1 {
					startIdx = i
				}
				endIdx = i + 1
			} else if startIdx != -1 {
				break
			}
		}

		if startIdx == -1 {
			return
		}

		vectorsInRange := endIdx - startIdx
		numWorkers := r.config.NumWorkers
		if numWorkers > vectorsInRange {
			numWorkers = vectorsInRange
		}

		vectorsPerWorker := (vectorsInRange + numWorkers - 1) / numWorkers

		resultCh := make(chan []SearchResult, numWorkers)
		done := make(chan struct{})
		actualWorkers := 0

		for i := 0; i < numWorkers; i++ {
			wStart := startIdx + i*vectorsPerWorker
			wEnd := wStart + vectorsPerWorker
			if wEnd > endIdx {
				wEnd = endIdx
			}
			if wStart >= wEnd {
				break
			}

			actualWorkers++
			go func(start, end int) {
				defer func() { recover() }()
				topK := r.scanChunkTQ(data, tqQuery, start, end, k, vectorSize, revMap)
				select {
				case resultCh <- topK:
				case <-done:
				}
			}(wStart, wEnd)
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

// scanChunkTQ scans a chunk of TQ vectors and returns local top-k results.
func (r *VectorRegistry) scanChunkTQ(data []byte, query []byte, startIdx, endIdx, k int, vectorSize int, revMap []uint64) []SearchResult {
	h := make(scoreHeap, 0, k)
	dim := r.config.FullDim
	tqCfg := r.tqConfig

	for idx := startIdx; idx < endIdx; idx++ {
		offset := idx * vectorSize
		if offset+vectorSize > len(data) {
			break
		}
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
