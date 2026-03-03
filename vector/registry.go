package vector

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// VectorRegistry holds compressed MRL vectors in RAM for fast search.
// Vectors are scalar-quantized to int8 (75% memory reduction vs float32).
// Full vectors are persisted to BadgerDB for potential retrieval.
type VectorRegistry struct {
	// data is a FLAT BUFFER storing all 64-d int8 vectors contiguously.
	// Size = NumVectors * 64 bytes (4x smaller than float32)
	// Vector[i] corresponds to data[i*64 : (i+1)*64]
	// Pre-allocated with large capacity to avoid frequent slice growing.
	data []int8

	// pqData is a FLAT BUFFER storing all PQ-encoded vectors.
	// Size = NumVectors * PQCodeSize bytes
	pqData []byte

	// pqCodebook is the trained PQ codebook used for quantization
	pqCodebook *PQCodebook

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
		data:       make([]int8, 0, capacity),
		pqData:     make([]byte, 0, capacity/8), // 8 bytes per PQ code vs 64 for MRL
		pqCodebook: NewPQCodebook(),
		idMap:      make(map[uint64]uint32, 100000),
		revMap:     make([]uint64, 0, 100000),
		db:         db,
	}
}

// Add adds a full vector to the registry.
// The compressed 64-d version is scalar-quantized to int8 and stored in RAM,
// and the full vector is asynchronously persisted to disk.
func (r *VectorRegistry) Add(id uint64, fullVec []float32) error {
	if len(fullVec) != FullDim {
		slog.Error("invalid vector dimension",
			"id", id,
			"expected", FullDim,
			"got", len(fullVec),
		)
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", FullDim, len(fullVec))
	}

	// Process to get 64-d MRL vector (normalized)
	mrlVec := ProcessMRL(fullVec)

	// Quantize to int8 for efficient storage
	quantized := Quantize(mrlVec)

	// Encode to PQ if trained
	var pqCoded []byte
	r.mu.RLock()
	if r.pqCodebook.Trained {
		pqCoded = r.pqCodebook.Encode(mrlVec)
	} else {
		pqCoded = make([]byte, PQCodeSize)
	}
	r.mu.RUnlock()

	r.mu.Lock()

	// Check if ID already exists
	if idx, exists := r.idMap[id]; exists {
		// Overwrite existing vector in place
		copy(r.data[idx*MRLDim:(idx+1)*MRLDim], quantized)
		copy(r.pqData[idx*PQCodeSize:(idx+1)*PQCodeSize], pqCoded)
		r.mu.Unlock()
		slog.Debug("vector updated",
			"id", id,
			"index", idx,
		)
	} else {
		// Append new vector
		idx := uint32(len(r.revMap))
		r.idMap[id] = idx
		r.revMap = append(r.revMap, id)
		r.data = append(r.data, quantized...)
		r.pqData = append(r.pqData, pqCoded...)
		r.mu.Unlock()
		slog.Debug("vector added",
			"id", id,
			"index", idx,
			"totalVectors", len(r.revMap),
		)
	}

	// Async disk write for full vector
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.persistFullVector(id, fullVec)
	}()

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

// Delete removes a vector from the registry by ID.
// Returns true if the vector was found and deleted, false if not found.
func (r *VectorRegistry) Delete(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, exists := r.idMap[id]
	if !exists {
		return false
	}

	lastIdx := uint32(len(r.revMap) - 1)
	lastID := r.revMap[lastIdx]

	if idx != lastIdx {
		r.revMap[idx] = lastID
		r.idMap[lastID] = idx

		srcOffset := int(lastIdx) * MRLDim
		dstOffset := int(idx) * MRLDim
		copy(r.data[dstOffset:dstOffset+MRLDim], r.data[srcOffset:srcOffset+MRLDim])

		srcPqOffset := int(lastIdx) * PQCodeSize
		dstPqOffset := int(idx) * PQCodeSize
		copy(r.pqData[dstPqOffset:dstPqOffset+PQCodeSize], r.pqData[srcPqOffset:srcPqOffset+PQCodeSize])
	}

	r.revMap = r.revMap[:lastIdx]
	delete(r.idMap, id)
	r.data = r.data[:len(r.data)-MRLDim]
	r.pqData = r.pqData[:len(r.pqData)-PQCodeSize]

	slog.Debug("vector deleted", "id", id, "remaining", len(r.revMap))

	return true
}

// HasVector checks if a vector exists for the given ID.
func (r *VectorRegistry) HasVector(id uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.idMap[id]
	return exists
}

// TrainPQ trains the PQ codebook using randomly sampled vectors and encodes all existing vectors.
func (r *VectorRegistry) TrainPQ(sampleSize int) error {
	r.mu.RLock()
	numVectors := len(r.revMap)
	r.mu.RUnlock()

	if numVectors < PQKClusters*10 {
		return fmt.Errorf("not enough vectors to train PQ (need at least %d, have %d)", PQKClusters*10, numVectors)
	}

	if sampleSize > numVectors {
		sampleSize = numVectors
	}
	if sampleSize < PQKClusters*10 {
		sampleSize = PQKClusters*10
	}

	slog.Info("training PQ codebook", "numVectors", numVectors, "sampleSize", sampleSize)

	r.mu.RLock()
	// Pick random unique indices
	indices := rand.Perm(numVectors)
	if len(indices) > sampleSize {
		indices = indices[:sampleSize]
	}

	samples := make([][]float32, 0, len(indices))
	idsToSample := make([]uint64, len(indices))
	for i, idx := range indices {
		idsToSample[i] = r.revMap[idx]
	}
	r.mu.RUnlock()

	for _, id := range idsToSample {
		fullVec, err := r.GetFullVector(id)
		if err != nil {
			continue // skip missing
		}
		mrlVec := ProcessMRL(fullVec)
		samples = append(samples, mrlVec)
	}

	if len(samples) < PQKClusters*10 {
		return fmt.Errorf("failed to load enough samples for PQ training")
	}

	// Train external codebook first
	newCodebook := NewPQCodebook()
	newCodebook.Train(samples)

	if !newCodebook.Trained {
		return fmt.Errorf("PQ training failed internally")
	}

	// Now encode all vectors
	slog.Info("encoding all vectors with trained PQ codebook")
	r.mu.RLock()
	allVectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		offset := i * MRLDim
		allVectors[i] = Dequantize(r.data[offset : offset+MRLDim])
	}
	r.mu.RUnlock()

	newPqData := make([]byte, numVectors*PQCodeSize)
	for i, vec := range allVectors {
		code := newCodebook.Encode(vec)
		offset := i * PQCodeSize
		copy(newPqData[offset:offset+PQCodeSize], code)
	}

	// Swap in the new codebook and data
	r.mu.Lock()
	r.pqCodebook = newCodebook
	r.pqData = newPqData
	r.mu.Unlock()

	slog.Info("PQ training complete")
	return nil
}
