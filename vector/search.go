package vector

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"iter"
	"log/slog"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

type SearchResult struct {
	ID    uint64
	Score float32
}

type scoreIndex struct {
	score float32
	id    uint64
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
// Fast path: parallel mmap scan if vectors are loaded in RAM.
// Fallback: streams from BadgerDB for cold start.
func (r *VectorRegistry) Search(queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return r.SearchWithFilter(queryVec, k, 0)
}

// SearchWithFilter returns an iterator of top-k most similar vectors matching a semantic hash.
// Fast path: parallel mmap scan if vectors are loaded in RAM.
// Fallback: streams from BadgerDB for cold start.
func (r *VectorRegistry) SearchWithFilter(queryVec []float32, k int, filterHash uint8) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		if k <= 0 {
			return
		}

		r.mu.RLock()
		numVectors := r.totalVectors
		hasVectors := numVectors > 0
		vectorSize := r.vectorSize
		hybridCfg := r.hybridCfg
		fullDim := r.config.FullDim
		numWorkers := r.config.NumWorkers
		revMap := *r.revMap.Load()

		// Capture function to access vector slots
		getSlot := func(idx int) []byte { return r.getVectorSlice(idx) }
		r.mu.RUnlock()

		if !hasVectors {
			// Cold start: stream from Badger
			r.searchFromBadger(queryVec, k, filterHash, hybridCfg, fullDim, yield)
			return
		}

		slog.Debug("mmap-fast vector search",
			"vectorCount", numVectors,
			"k", k,
		)

		hybridQuery := QuantizeHybrid(queryVec, hybridCfg)

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
				topK := scanChunkHybrid(getSlot, hybridQuery, start, end, k, vectorSize, revMap, filterHash, fullDim, hybridCfg)
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
// Always uses Badger prefix scan — LSM-level filtering is faster than scanning all mmap vectors.
func (r *VectorRegistry) SearchInTopic(topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		if k <= 0 {
			return
		}

		slog.Debug("Badger-streaming topic-aware vector search started",
			"topicID", topicID,
			"vectorCount", r.Count(),
			"k", k,
		)

		r.mu.RLock()
		hybridCfg := r.hybridCfg
		fullDim := r.config.FullDim
		r.mu.RUnlock()

		hybridQuery := QuantizeHybrid(queryVec, hybridCfg)

		txn := r.db.NewTransaction(false)
		defer txn.Discard()

		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true
		it := txn.NewIterator(itOpts)
		defer it.Close()

		// Topic prefix: [0x11][topicID << 40] — Badger LSM-tree prunes unrelated topics
		topicPrefix := buildTopicVectorPrefix(topicID)
		topicEnd := buildTopicVectorEnd(topicID)
		h := make(scoreHeap, 0, k)

		for it.Seek(topicPrefix); it.Valid(); it.Next() {
			key := it.Item().Key()
			if bytes.Compare(key, topicEnd) >= 0 {
				break
			}
			if len(key) != keys.ChunkKeySize || key[0] != keys.VectorFullPrefix {
				continue
			}

			// Extract ID from key: [0x11][id:8]
			id := binary.BigEndian.Uint64(key[1:])

			var score float32
			err := it.Item().Value(func(val []byte) error {
				if len(val) < 1 {
					return nil
				}
				vecData := val[1:]
				score = DotProductHybrid(vecData, hybridQuery, fullDim, hybridCfg)
				return nil
			})
			if err != nil {
				yield(SearchResult{}, err)
				return
			}

			// Maintain top-k heap
			if len(h) < k {
				heap.Push(&h, scoreIndex{score: score, id: id})
			} else if score > h[0].score {
				h[0] = scoreIndex{score: score, id: id}
				heap.Fix(&h, 0)
			}
		}

		// Sort by descending score and yield
		results := extractResults(h)
		for _, sr := range results {
			if !yield(sr, nil) {
				return
			}
		}
	}
}

// searchFromBadger streams all vectors from Badger and maintains a top-k heap.
// Used as fallback when mmap cache is empty (cold start).
func (r *VectorRegistry) searchFromBadger(queryVec []float32, k int, filterHash uint8, hybridCfg *HybridConfig, fullDim int, yield func(SearchResult, error) bool) {
	slog.Debug("Badger-streaming vector search (cold start)",
		"k", k,
	)

	hybridQuery := QuantizeHybrid(queryVec, hybridCfg)

	txn := r.db.NewTransaction(false)
	defer txn.Discard()

	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchValues = true
	it := txn.NewIterator(itOpts)
	defer it.Close()

	// Prefix: [0x11] — all vectors
	prefix := []byte{keys.VectorFullPrefix}
	h := make(scoreHeap, 0, k)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		vecKey := item.Key()
		if len(vecKey) != keys.ChunkKeySize {
			continue
		}

		// Extract ID from key: [0x11][id:8]
		id := binary.BigEndian.Uint64(vecKey[1:])

		var score float32
		err := item.Value(func(val []byte) error {
			if len(val) < 1 {
				return nil
			}
			hashByte := val[0]
			if filterHash != 0 && hashByte != filterHash {
				return nil
			}
			vecData := val[1:]
			score = DotProductHybrid(vecData, hybridQuery, fullDim, hybridCfg)
			return nil
		})
		if err != nil {
			yield(SearchResult{}, err)
			return
		}

		// Maintain top-k heap
		if len(h) < k {
			heap.Push(&h, scoreIndex{score: score, id: id})
		} else if score > h[0].score {
			h[0] = scoreIndex{score: score, id: id}
			heap.Fix(&h, 0)
		}
	}

	// Sort by descending score and yield
	results := extractResults(h)
	for _, sr := range results {
		if !yield(sr, nil) {
			return
		}
	}
}

// buildTopicVectorPrefix builds a Badger prefix for streaming vectors of a specific topic.
// Key format: [0x11][TopicID:24][LocalID:40] — topic is in the upper 24 bits of the ID.
func buildTopicVectorPrefix(topicID uint32) []byte {
	prefix := make([]byte, 5)
	prefix[0] = keys.VectorFullPrefix
	binary.BigEndian.PutUint32(prefix[1:], topicID)
	return prefix
}

// buildTopicVectorEnd builds the exclusive upper bound for a topic's vector range.
func buildTopicVectorEnd(topicID uint32) []byte {
	end := make([]byte, 5)
	end[0] = keys.VectorFullPrefix
	binary.BigEndian.PutUint32(end[1:], topicID+1)
	return end
}

func extractResults(h scoreHeap) []SearchResult {
	results := make([]SearchResult, len(h))
	for i := len(h) - 1; i >= 0; i-- {
		si := heap.Pop(&h).(scoreIndex)
		results[i] = SearchResult{
			ID:    si.id,
			Score: si.score,
		}
	}
	return results
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
			h = append(h, scoreIndex{score: score, id: revMap[idx]})
			if len(h) == k {
				heap.Init(&h)
			}
		} else if score > h[0].score {
			h[0] = scoreIndex{score: score, id: revMap[idx]}
			heap.Fix(&h, 0)
		}
	}

	results := make([]SearchResult, len(h))
	for i, si := range h {
		results[i] = SearchResult{
			ID:    si.id,
			Score: si.score,
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
