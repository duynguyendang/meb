package vector

import (
	"fmt"
	"log/slog"
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
		data:   make([]int8, 0, capacity),
		idMap:  make(map[uint64]uint32, 100000),
		revMap: make([]uint64, 0, 100000),
		db:     db,
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

	r.mu.Lock()

	// Check if ID already exists
	if idx, exists := r.idMap[id]; exists {
		// Overwrite existing vector in place
		copy(r.data[idx*MRLDim:(idx+1)*MRLDim], quantized)
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
