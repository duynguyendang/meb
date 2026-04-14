package vector

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"syscall"

	"github.com/duynguyendang/meb/keys"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	FullDim         int
	HybridBitWidth  int
	HybridBlockSize int
	NumWorkers      int
	VectorCapacity  int
	InitialCapacity int
	SegmentDir      string
	SegmentSize     int
}

func DefaultConfig() *Config {
	return &Config{
		FullDim:         1536,
		HybridBitWidth:  8,
		HybridBlockSize: 32,
		NumWorkers:      4,
		VectorCapacity:  25 * 1024 * 1024,
		InitialCapacity: 100000,
		SegmentSize:     64 << 20,
	}
}

func (c *Config) Validate() error {
	if c.FullDim <= 0 {
		return fmt.Errorf("FullDim must be positive")
	}
	if c.HybridBitWidth != 4 && c.HybridBitWidth != 8 {
		return fmt.Errorf("HybridBitWidth must be 4 or 8")
	}
	if c.HybridBlockSize <= 0 || c.HybridBlockSize%8 != 0 {
		return fmt.Errorf("HybridBlockSize must be positive and divisible by 8")
	}
	if c.NumWorkers <= 0 {
		return fmt.Errorf("NumWorkers must be positive")
	}
	if c.VectorCapacity <= 0 {
		return fmt.Errorf("VectorCapacity must be positive")
	}
	if c.InitialCapacity <= 0 {
		return fmt.Errorf("InitialCapacity must be positive")
	}
	return nil
}

func (c *Config) HybridConfig() *HybridConfig {
	return &HybridConfig{
		BitWidth:  c.HybridBitWidth,
		BlockSize: c.HybridBlockSize,
	}
}

// VectorRegistry holds Hybrid (FWHT + block-wise) compressed vectors for fast search.
type VectorRegistry struct {
	config     *Config
	hybridCfg  *HybridConfig
	vectorSize int

	segments          []*mmapSegment
	vectorsPerSegment int
	totalVectors      int

	idMap  map[uint64]uint32
	revMap []uint64

	db *badger.DB
	mu sync.RWMutex
	wg sync.WaitGroup
}

const hashSize = 1 // 1 byte for semantic hash filter per vector entry

func NewRegistry(db *badger.DB, cfg *Config) *VectorRegistry {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("invalid vector config: %v", err))
	}

	hybridCfg := cfg.HybridConfig()
	vSize := HybridVectorSize(cfg.FullDim, hybridCfg) + hashSize

	segSize := cfg.SegmentSize
	if segSize <= 0 {
		segSize = 64 << 20
	}
	vectorsPerSeg := segSize / vSize
	if vectorsPerSeg < 1 {
		vectorsPerSeg = 1
	}

	r := &VectorRegistry{
		config:            cfg,
		hybridCfg:         hybridCfg,
		vectorSize:        vSize,
		vectorsPerSegment: vectorsPerSeg,
		idMap:             make(map[uint64]uint32, cfg.InitialCapacity),
		revMap:            make([]uint64, 0, cfg.InitialCapacity),
		db:                db,
	}

	return r
}

// Reset clears all vectors, mappings, and segment data from the registry.
// This is used by MEBStore.Reset() to ensure a clean state.
func (r *VectorRegistry) Reset() error {
	// Wait for pending vector persistence goroutines before resetting state
	r.wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()

	slog.Info("resetting vector registry",
		"totalVectors", r.totalVectors,
		"segments", len(r.segments),
	)

	// Clear mappings
	r.idMap = make(map[uint64]uint32, r.config.InitialCapacity)
	r.revMap = make([]uint64, 0, r.config.InitialCapacity)
	r.totalVectors = 0

	// Unmap and close segments
	for _, seg := range r.segments {
		if seg.data != nil {
			syscall.Munmap(seg.data)
			seg.data = nil
		}
		if seg.file != nil {
			// Truncate file to 0 and close
			seg.file.Truncate(0)
			seg.file.Close()
		}
	}

	// Clear segment list
	r.segments = r.segments[:0]

	slog.Info("vector registry reset complete")
	return nil
}

// ensureCapacity grows the segment list if needed to hold the required number of vectors.
func (r *VectorRegistry) ensureCapacity(totalVectors int) error {
	requiredSegs := (totalVectors + r.vectorsPerSegment - 1) / r.vectorsPerSegment
	for len(r.segments) < requiredSegs {
		segIdx := len(r.segments)
		segBytes := r.vectorsPerSegment * r.vectorSize
		seg, err := newMmapSegment(r.config.SegmentDir, segIdx, segBytes)
		if err != nil {
			return fmt.Errorf("failed to create segment %d: %w", segIdx, err)
		}
		r.segments = append(r.segments, seg)
	}
	return nil
}

// getVectorSlice returns a writable slice for the vector at the given index.
func (r *VectorRegistry) getVectorSlice(idx int) []byte {
	segIdx := idx / r.vectorsPerSegment
	offset := (idx % r.vectorsPerSegment) * r.vectorSize
	return r.segments[segIdx].data[offset : offset+r.vectorSize]
}

func (r *VectorRegistry) Add(id uint64, fullVec []float32) error {
	return r.AddWithHash(id, fullVec, 0)
}

// AddWithHash adds a vector with a semantic hash byte for early filtering during search.
func (r *VectorRegistry) AddWithHash(id uint64, fullVec []float32, semanticHash uint8) error {
	if len(fullVec) != r.config.FullDim {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", r.config.FullDim, len(fullVec))
	}

	hybridData := QuantizeHybrid(fullVec, r.hybridCfg)
	hybridSize := len(hybridData)

	r.mu.Lock()
	if idx, exists := r.idMap[id]; exists {
		slot := r.getVectorSlice(int(idx))
		slot[0] = semanticHash
		copy(slot[hashSize:hashSize+hybridSize], hybridData)
		r.mu.Unlock()
	} else {
		newTotal := r.totalVectors + 1
		if err := r.ensureCapacity(newTotal); err != nil {
			r.mu.Unlock()
			return err
		}

		idx := uint32(r.totalVectors)
		r.idMap[id] = idx
		r.revMap = append(r.revMap, id)

		slot := r.getVectorSlice(int(idx))
		slot[0] = semanticHash
		copy(slot[hashSize:hashSize+hybridSize], hybridData)

		r.totalVectors = newTotal
		r.mu.Unlock()
	}

	r.wg.Add(1)
	go func(vecCopy []float32) {
		defer r.wg.Done()
		key := keys.EncodeVectorFullKey(id)
		value := make([]byte, r.config.FullDim*4)
		for i, v := range vecCopy {
			binary.LittleEndian.PutUint32(value[i*4:(i+1)*4], math.Float32bits(v))
		}
		if err := r.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		}); err != nil {
			slog.Error("failed to persist full vector to BadgerDB", "id", id, "error", err)
		}
	}(append([]float32(nil), fullVec...))

	return nil
}

func (r *VectorRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.revMap)
}

func (r *VectorRegistry) Close() error {
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	for _, seg := range r.segments {
		if err := seg.close(); err != nil {
			lastErr = err
		}
	}
	r.segments = nil
	return lastErr
}

func (r *VectorRegistry) Delete(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, exists := r.idMap[id]
	if !exists {
		return false
	}

	lastIdx := uint32(r.totalVectors - 1)
	lastID := r.revMap[lastIdx]

	if idx != lastIdx {
		r.revMap[idx] = lastID
		r.idMap[lastID] = idx

		srcSlot := r.getVectorSlice(int(lastIdx))
		dstSlot := r.getVectorSlice(int(idx))
		copy(dstSlot, srcSlot)
	}

	r.revMap = r.revMap[:lastIdx]
	delete(r.idMap, id)
	r.totalVectors--

	return true
}

func (r *VectorRegistry) HasVector(id uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.idMap[id]
	return exists
}

// GetTQVector returns the TQ-compressed vector data at the given index (skipping hash byte).
func (r *VectorRegistry) GetTQVector(idx int) []byte {
	if idx < 0 || idx >= r.totalVectors {
		return nil
	}
	slot := r.getVectorSlice(idx)
	return slot[hashSize:]
}

func (r *VectorRegistry) HybridConfig() *HybridConfig {
	return r.hybridCfg
}

func (r *VectorRegistry) VectorSize() int {
	return r.vectorSize
}

func (r *VectorRegistry) GetFullVector(id uint64) ([]float32, error) {
	key := keys.EncodeVectorFullKey(id)

	var fullVec []float32
	fullDim := r.config.FullDim
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			fullVec = make([]float32, fullDim)
			for i := 0; i < fullDim; i++ {
				bits := binary.LittleEndian.Uint32(val[i*4 : (i+1)*4])
				fullVec[i] = math.Float32frombits(bits)
			}
			return nil
		})
	})

	return fullVec, err
}

// snapshotChunkSize is the maximum number of vectors per snapshot chunk key.
// This avoids allocating one giant contiguous buffer for all vectors.
const snapshotChunkSize = 10000

func (r *VectorRegistry) SaveSnapshot() error {
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()

	numVectors := len(r.revMap)
	slog.Info("saving TQ vector snapshot",
		"vectorCount", numVectors,
		"segments", len(r.segments),
	)

	// Sync all mmap segments to disk
	for _, seg := range r.segments {
		if err := seg.sync(); err != nil {
			slog.Warn("segment sync failed", "error", err)
		}
	}

	batch := r.db.NewWriteBatch()
	defer batch.Cancel()

	// Save vectors in chunked keys to avoid a single giant allocation
	for start := 0; start < numVectors; start += snapshotChunkSize {
		end := start + snapshotChunkSize
		if end > numVectors {
			end = numVectors
		}
		chunkBytes := (end - start) * r.vectorSize
		buf := make([]byte, chunkBytes)
		for i := start; i < end; i++ {
			slot := r.getVectorSlice(i)
			off := (i - start) * r.vectorSize
			copy(buf[off:off+r.vectorSize], slot)
		}
		key := fmt.Appendf(nil, "sys:tq:vectors:%d", start/snapshotChunkSize)
		if err := batch.Set(key, buf); err != nil {
			return fmt.Errorf("failed to save TQ vector chunk %d: %w", start/snapshotChunkSize, err)
		}
	}

	// Save IDs in chunked keys
	for start := 0; start < numVectors; start += snapshotChunkSize {
		end := start + snapshotChunkSize
		if end > numVectors {
			end = numVectors
		}
		idsBytes := make([]byte, (end-start)*8)
		for i := start; i < end; i++ {
			binary.BigEndian.PutUint64(idsBytes[(i-start)*8:(i-start+1)*8], r.revMap[i])
		}
		key := fmt.Appendf(nil, "sys:tq:ids:%d", start/snapshotChunkSize)
		if err := batch.Set(key, idsBytes); err != nil {
			return fmt.Errorf("failed to save TQ id chunk %d: %w", start/snapshotChunkSize, err)
		}
	}

	// Save metadata: total count
	metaBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(metaBuf, uint64(numVectors))
	if err := batch.Set([]byte("sys:tq:meta"), metaBuf); err != nil {
		return fmt.Errorf("failed to save TQ snapshot meta: %w", err)
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	// Clean up legacy monolithic keys if they exist
	r.db.Update(func(txn *badger.Txn) error {
		txn.Delete([]byte("sys:tq:vectors"))
		txn.Delete([]byte("sys:tq:ids"))
		return nil
	})

	slog.Info("TQ vector snapshot saved", "vectorCount", numVectors)
	return nil
}

func (r *VectorRegistry) LoadSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.Info("loading TQ vector snapshot")

	err := r.db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get([]byte("sys:tq:meta"))
		if err == nil {
			return r.loadChunkedSnapshot(txn, metaItem)
		}
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to load TQ snapshot meta: %w", err)
		}

		return r.loadLegacySnapshot(txn)
	})

	if err != nil {
		return err
	}

	if r.totalVectors > 0 {
		slog.Info("TQ vector snapshot loaded",
			"vectorCount", r.totalVectors,
			"vectorSize", r.vectorSize,
		)
	}

	return nil
}

func (r *VectorRegistry) loadChunkedSnapshot(txn *badger.Txn, metaItem *badger.Item) error {
	var numTotal uint64
	if err := metaItem.Value(func(val []byte) error {
		numTotal = binary.BigEndian.Uint64(val)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to read snapshot meta: %w", err)
	}

	numVectors := int(numTotal)
	if numVectors == 0 {
		return nil
	}

	if err := r.ensureCapacity(numVectors); err != nil {
		return fmt.Errorf("failed to ensure capacity for snapshot: %w", err)
	}

	// Load vector data chunks
	vecIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := fmt.Appendf(nil, "sys:tq:vectors:%d", chunkIdx)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to load TQ vector chunk %d: %w", chunkIdx, err)
		}
		if err := item.Value(func(val []byte) error {
			chunkVecs := len(val) / r.vectorSize
			for i := 0; i < chunkVecs; i++ {
				slot := r.getVectorSlice(vecIdx)
				copy(slot, val[i*r.vectorSize:(i+1)*r.vectorSize])
				vecIdx++
			}
			return nil
		}); err != nil {
			return err
		}
	}
	r.totalVectors = numVectors

	// Load ID chunks
	r.revMap = make([]uint64, numVectors)
	idIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := fmt.Appendf(nil, "sys:tq:ids:%d", chunkIdx)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to load TQ id chunk %d: %w", chunkIdx, err)
		}
		if err := item.Value(func(val []byte) error {
			chunkIDs := len(val) / 8
			for i := 0; i < chunkIDs; i++ {
				r.revMap[idIdx] = binary.BigEndian.Uint64(val[i*8 : (i+1)*8])
				idIdx++
			}
			return nil
		}); err != nil {
			return err
		}
	}

	r.idMap = make(map[uint64]uint32, numVectors)
	for idx, id := range r.revMap {
		r.idMap[id] = uint32(idx)
	}

	return nil
}

func (r *VectorRegistry) loadLegacySnapshot(txn *badger.Txn) error {
	var vectorsBytes, idsBytes []byte

	item, err := txn.Get([]byte("sys:tq:vectors"))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			slog.Info("no existing TQ vector snapshot found")
			return nil
		}
		return fmt.Errorf("failed to load TQ vectors: %w", err)
	}
	if err := item.Value(func(val []byte) error {
		vectorsBytes = make([]byte, len(val))
		copy(vectorsBytes, val)
		return nil
	}); err != nil {
		return err
	}

	item, err = txn.Get([]byte("sys:tq:ids"))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return fmt.Errorf("failed to load TQ ids: %w", err)
	}
	if err := item.Value(func(val []byte) error {
		idsBytes = make([]byte, len(val))
		copy(idsBytes, val)
		return nil
	}); err != nil {
		return err
	}

	if vectorsBytes == nil || idsBytes == nil {
		return nil
	}

	numVectors := len(vectorsBytes) / r.vectorSize

	if err := r.ensureCapacity(numVectors); err != nil {
		return fmt.Errorf("failed to ensure capacity for snapshot: %w", err)
	}

	for i := 0; i < numVectors; i++ {
		slot := r.getVectorSlice(i)
		copy(slot, vectorsBytes[i*r.vectorSize:(i+1)*r.vectorSize])
	}
	r.totalVectors = numVectors

	r.revMap = make([]uint64, numVectors)
	for i := 0; i < numVectors; i++ {
		r.revMap[i] = binary.BigEndian.Uint64(idsBytes[i*8 : (i+1)*8])
	}

	r.idMap = make(map[uint64]uint32, numVectors)
	for idx, id := range r.revMap {
		r.idMap[id] = uint32(idx)
	}

	slog.Info("TQ vector snapshot loaded (legacy format)",
		"vectorCount", numVectors,
		"vectorSize", r.vectorSize,
	)

	return nil
}
