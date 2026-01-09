package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// SearchResult represents a single search result with similarity score.
type SearchResult struct {
	ID     uint64  // GraphID
	Score  float32 // Cosine similarity (higher is better)
}

// VectorRegistry holds compressed MRL vectors in RAM for fast search.
// Vectors are scalar-quantized to int8 (75% memory reduction vs float32).
// Full vectors are persisted to BadgerDB for potential retrieval.
type VectorRegistry struct {
	// data is a FLAT BUFFER storing all 64-d int8 vectors contiguously.
	// Size = NumVectors * 64 bytes (4x smaller than float32)
	// Vector[i] corresponds to data[i*64 : (i+1)*64]
	// Pre-allocated with large capacity to avoid frequent slice growing.
	data []int8

	// idMap maps GraphID -> Internal Index (uint32)
	idMap map[uint64]uint32

	// revMap maps Internal Index -> GraphID
	revMap []uint64

	// db is the BadgerDB instance for persistence
	db *badger.DB

	// mu protects concurrent access to the registry
	mu sync.RWMutex

	// wg tracks async disk write operations
	wg sync.WaitGroup
}

// NewRegistry creates a new VectorRegistry with pre-allocated memory.
// Allocates 25MB upfront (~1.6M int8 vectors) to avoid frequent reallocations.
// With int8 quantization, we can store 4x more vectors in the same memory.
func NewRegistry(db *badger.DB) *VectorRegistry {
	// Pre-allocate ~25MB for int8 vectors (25 * 1024 * 1024 bytes)
	// This can hold approximately 409,600 vectors (26,214,400 / 64)
	capacity := 25 * 1024 * 1024 // 25MB in bytes

	return &VectorRegistry{
		data:    make([]int8, 0, capacity),
		idMap:   make(map[uint64]uint32, 100000),
		revMap:  make([]uint64, 0, 100000),
		db:      db,
	}
}

// Add adds a full vector to the registry.
// The compressed 64-d version is scalar-quantized to int8 and stored in RAM,
// and the full vector is asynchronously persisted to disk.
func (r *VectorRegistry) Add(id uint64, fullVec []float32) error {
	if len(fullVec) != FullDim {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", FullDim, len(fullVec))
	}

	// Process to get 64-d MRL vector (normalized)
	mrlVec := ProcessMRL(fullVec)

	// Quantize to int8 for efficient storage
	quantized := Quantize(mrlVec)

	r.mu.Lock()

	// Check if ID already exists
	if idx, exists := r.idMap[id]; exists {
		// Overwrite existing vector in place
		copy(r.data[idx*MRLDim:(idx+1)*MRLDim], quantized)
		r.mu.Unlock()
	} else {
		// Append new vector
		idx := uint32(len(r.revMap))
		r.idMap[id] = idx
		r.revMap = append(r.revMap, id)
		r.data = append(r.data, quantized...)
		r.mu.Unlock()
	}

	// Async disk write for full vector
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.persistFullVector(id, fullVec)
	}()

	return nil
}

// persistFullVector writes the full 1536-d vector to BadgerDB.
// Key format: "vec:full:<BigEndianID>"
func (r *VectorRegistry) persistFullVector(id uint64, fullVec []float32) error {
	key := make([]byte, 1+8)
	key[0] = 0x10 // Prefix for full vectors
	binary.BigEndian.PutUint64(key[1:9], id)

	// Serialize vector to bytes (little-endian for performance)
	value := make([]byte, FullDim*4)
	for i, v := range fullVec {
		binary.LittleEndian.PutUint32(value[i*4:(i+1)*4], math.Float32bits(v))
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// GetFullVector retrieves the full 1536-d vector from disk.
func (r *VectorRegistry) GetFullVector(id uint64) ([]float32, error) {
	key := make([]byte, 1+8)
	key[0] = 0x10 // Prefix for full vectors
	binary.BigEndian.PutUint64(key[1:9], id)

	var fullVec []float32
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			fullVec = make([]float32, FullDim)
			for i := 0; i < FullDim; i++ {
				bits := binary.LittleEndian.Uint32(val[i*4 : (i+1)*4])
				fullVec[i] = math.Float32frombits(bits)
			}
			return nil
		})
	})

	return fullVec, err
}

// Search performs a parallel linear scan to find the top-k most similar vectors.
// Optimized with int8 scalar quantization and SIMD instructions.
func (r *VectorRegistry) Search(queryVec []float32, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

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

// SaveSnapshot persists the current RAM state to BadgerDB.
func (r *VectorRegistry) SaveSnapshot() error {
	// Wait for all async writes to complete
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Serialize int8 vectors (direct byte copy - very fast)
	vectorsBytes := make([]byte, len(r.data))
	for i, v := range r.data {
		vectorsBytes[i] = byte(v)
	}

	// Serialize revMap
	idsBytes := make([]byte, len(r.revMap)*8)
	for i, id := range r.revMap {
		binary.BigEndian.PutUint64(idsBytes[i*8:(i+1)*8], id)
	}

	batch := r.db.NewWriteBatch()
	defer batch.Cancel()

	// Save vectors snapshot
	if err := batch.Set([]byte("sys:mrl:vectors"), vectorsBytes); err != nil {
		return fmt.Errorf("failed to save vectors snapshot: %w", err)
	}

	// Save IDs snapshot
	if err := batch.Set([]byte("sys:mrl:ids"), idsBytes); err != nil {
		return fmt.Errorf("failed to save IDs snapshot: %w", err)
	}

	return batch.Flush()
}

// LoadSnapshot restores the RAM state from BadgerDB.
func (r *VectorRegistry) LoadSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var vectorsBytes, idsBytes []byte

	err := r.db.View(func(txn *badger.Txn) error {
		// Load vectors
		item, err := txn.Get([]byte("sys:mrl:vectors"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				// No snapshot exists
				return nil
			}
			return fmt.Errorf("failed to load vectors snapshot: %w", err)
		}

		if err := item.Value(func(val []byte) error {
			vectorsBytes = make([]byte, len(val))
			copy(vectorsBytes, val)
			return nil
		}); err != nil {
			return err
		}

		// Load IDs
		item, err = txn.Get([]byte("sys:mrl:ids"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("failed to load IDs snapshot: %w", err)
		}

		return item.Value(func(val []byte) error {
			idsBytes = make([]byte, len(val))
			copy(idsBytes, val)
			return nil
		})
	})

	if err != nil {
		return err
	}

	// No snapshot found
	if vectorsBytes == nil || idsBytes == nil {
		return nil
	}

	// Deserialize int8 vectors (direct byte copy - very fast)
	numVectors := len(vectorsBytes) / MRLDim
	r.data = make([]int8, numVectors*MRLDim)
	for i, v := range vectorsBytes {
		r.data[i] = int8(v)
	}

	// Deserialize revMap
	r.revMap = make([]uint64, numVectors)
	for i := 0; i < numVectors; i++ {
		r.revMap[i] = binary.BigEndian.Uint64(idsBytes[i*8 : (i+1)*8])
	}

	// Rebuild idMap
	r.idMap = make(map[uint64]uint32, numVectors)
	for idx, id := range r.revMap {
		r.idMap[id] = uint32(idx)
	}

	return nil
}

// Count returns the number of vectors in the registry.
func (r *VectorRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.revMap)
}

// Close waits for all async operations to complete.
func (r *VectorRegistry) Close() error {
	r.wg.Wait()
	return nil
}
