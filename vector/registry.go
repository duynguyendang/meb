package vector

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"

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

	idMap    map[uint64]uint32
	revMap   *atomic.Pointer[[]uint64] // RCU-style: atomic swap avoids full copy under RLock

	db *badger.DB
	mu sync.RWMutex

	// Tombstone support for deferred segment cleanup in Reset()
	tombstoneSegments []*mmapSegment
	tombstoneWG       sync.WaitGroup

	closed atomic.Bool
}

const hashSize = 1 // 1 byte for semantic hash filter per vector entry

// appendRevMap extends the revMap by one entry. O(1) when pre-allocated capacity allows.
// Must be called under mu.Lock().
func (r *VectorRegistry) appendRevMap(id uint64) {
	oldRev := r.revMap.Load()
	if cap(*oldRev) > len(*oldRev) {
		*oldRev = append(*oldRev, id)
		r.revMap.Store(oldRev)
	} else {
		newRev := make([]uint64, len(*oldRev)+1, len(*oldRev)*2)
		copy(newRev, *oldRev)
		newRev[len(*oldRev)] = id
		r.revMap.Store(&newRev)
	}
}

func NewRegistry(db *badger.DB, cfg *Config) *VectorRegistry {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("invalid vector config: %v", err))
	}

	hybridCfg := cfg.HybridConfig()
	paddedDim := nextPow2(cfg.FullDim)
	vSize := HybridVectorSize(paddedDim, hybridCfg) + hashSize

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
		revMap: func() *atomic.Pointer[[]uint64] {
			p := &atomic.Pointer[[]uint64]{}
			// Pre-allocate to VectorCapacity to avoid O(n²) copies during bulk load
			rev := make([]uint64, 0, cfg.VectorCapacity)
			p.Store(&rev)
			return p
		}(),
		db: db,
	}

	return r
}

// Reset clears all vectors, mappings, and segment data from the registry.
// Segment file IO (Munmap, Truncate, Close) is deferred for tombstoneBarrier
// (5 seconds) via a tombstone barrier to allow in-flight Search() calls to
// drain their mmap reads. Callers should wait 5s after Reset() before Close(),
// or use a synchronous ResetNow(ctx) variant for immediate teardown.
// Returns ErrClosed if the registry has already been closed.
func (r *VectorRegistry) Reset() error {
	if r.closed.Load() {
		return ErrClosed
	}

	r.mu.Lock()

	slog.Info("resetting vector registry",
		"totalVectors", r.totalVectors,
		"segments", len(r.segments),
	)

	// Move current segments to tombstone list
	if len(r.segments) > 0 {
		r.tombstoneSegments = append(r.tombstoneSegments, r.segments...)
	}

	// Clear mappings
	r.idMap = make(map[uint64]uint32, r.config.InitialCapacity)
	r.revMap.Store(&[]uint64{})
	r.totalVectors = 0
	r.segments = r.segments[:0]

	r.mu.Unlock()

	// Spawn background cleanup for tombstoned segments (if any pending)
	// If a previous cleanup is still running, it will handle these too
	// since we appended to tombstoneSegments.
	if len(r.tombstoneSegments) > 0 {
		// Wait for previous cleanup to finish before starting new one
		r.tombstoneWG.Wait()

		segsToClean := r.tombstoneSegments

		// Create new tombstone list for future resets
		r.tombstoneSegments = nil

		r.tombstoneWG.Add(1)
		go cleanupTombstonedSegments(segsToClean, &r.tombstoneWG)
	}

	slog.Info("vector registry reset complete",
		"tombstonedSegments", len(r.segments),
	)
	return nil
}

// ResetNow synchronously cleans up all tombstoned segments immediately.
// Blocks until all deferred cleanup completes.
func (r *VectorRegistry) ResetNow() {
	r.tombstoneWG.Wait()
}

// WaitTombstones blocks until all tombstoned segment cleanup completes.
func (r *VectorRegistry) WaitTombstones() {
	r.tombstoneWG.Wait()
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

// AddToMmapCache adds a vector to the in-memory mmap cache only (no Badger write).
// Used from post-commit paths where the Badger write was already done via a transaction.
func (r *VectorRegistry) AddToMmapCache(id uint64, fullVec []float32, semanticHash uint8) error {
	if len(fullVec) != r.config.FullDim {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", r.config.FullDim, len(fullVec))
	}

	hybridData := QuantizeHybrid(fullVec, r.hybridCfg)
	hybridSize := len(hybridData)

	if hashSize+hybridSize > r.vectorSize {
		panic(fmt.Sprintf("vector slot too small: need %d, have %d (dim=%d, padded=%d)",
			hashSize+hybridSize, r.vectorSize, r.config.FullDim, nextPow2(r.config.FullDim)))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return ErrClosed
	}

	if idx, exists := r.idMap[id]; exists {
		slot := r.getVectorSlice(int(idx))
		slot[0] = semanticHash
		copy(slot[hashSize:hashSize+hybridSize], hybridData)
		return nil
	}

	newTotal := r.totalVectors + 1
	if err := r.ensureCapacity(newTotal); err != nil {
		return err
	}

	idx := uint32(r.totalVectors)
	r.idMap[id] = idx
	r.appendRevMap(id)

	slot := r.getVectorSlice(int(idx))
	slot[0] = semanticHash
	copy(slot[hashSize:hashSize+hybridSize], hybridData)

	r.totalVectors = newTotal
	return nil
}

// AddWithHash adds a vector with a semantic hash byte for early filtering during search.
// The compressed vector is written to Badger as the primary store, and also stored
// in mmap segments as an in-memory cache for low-latency warm data access.
func (r *VectorRegistry) AddWithHash(id uint64, fullVec []float32, semanticHash uint8) error {
	if len(fullVec) != r.config.FullDim {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", r.config.FullDim, len(fullVec))
	}

	hybridData := QuantizeHybrid(fullVec, r.hybridCfg)
	hybridSize := len(hybridData)

	if hashSize+hybridSize > r.vectorSize {
		panic(fmt.Sprintf("vector slot too small: need %d, have %d (dim=%d, padded=%d)",
			hashSize+hybridSize, r.vectorSize, r.config.FullDim, nextPow2(r.config.FullDim)))
	}

	// Build value: [hashByte:1][hybridData:N]
	valueBuf := make([]byte, 1+hybridSize)
	valueBuf[0] = semanticHash
	copy(valueBuf[1:], hybridData)

	// Write compressed vector to Badger immediately (primary store)
	vecKey := keys.EncodeVectorFullKey(id)
	if err := r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(vecKey, valueBuf)
	}); err != nil {
		return fmt.Errorf("failed to persist vector to BadgerDB: %w", err)
	}

	return r.addMmapOnly(id, hybridData, semanticHash)
}

// addMmapOnly writes pre-quantized hybrid data directly to the mmap cache.
// Skips re-quantization — called from AddWithHash where hybridData is already computed.
func (r *VectorRegistry) addMmapOnly(id uint64, hybridData []byte, semanticHash uint8) error {
	hybridSize := len(hybridData)

	if hashSize+hybridSize > r.vectorSize {
		panic(fmt.Sprintf("vector slot too small: need %d, have %d (dim=%d, padded=%d)",
			hashSize+hybridSize, r.vectorSize, r.config.FullDim, nextPow2(r.config.FullDim)))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return ErrClosed
	}

	if idx, exists := r.idMap[id]; exists {
		slot := r.getVectorSlice(int(idx))
		slot[0] = semanticHash
		copy(slot[hashSize:hashSize+hybridSize], hybridData)
		return nil
	}

	newTotal := r.totalVectors + 1
	if err := r.ensureCapacity(newTotal); err != nil {
		return err
	}

	idx := uint32(r.totalVectors)
	r.idMap[id] = idx
	r.appendRevMap(id)

	slot := r.getVectorSlice(int(idx))
	slot[0] = semanticHash
	copy(slot[hashSize:hashSize+hybridSize], hybridData)

	r.totalVectors = newTotal
	return nil
}

func (r *VectorRegistry) Count() int {
	return len(*r.revMap.Load())
}

func (r *VectorRegistry) Close() error {
	r.closed.Store(true)

	// Close current segments without holding the lock during IO.
	r.mu.Lock()
	segments := r.segments
	r.segments = nil
	r.mu.Unlock()

	var lastErr error
	for _, seg := range segments {
		if err := seg.close(); err != nil {
			lastErr = err
		}
	}

	// Wait for any in-flight tombstone cleanup goroutine. We release the
	// lock during Wait so the goroutine can access tombstoneSegments.
	r.tombstoneWG.Wait()

	// After Wait, the goroutine has finished. Close any segments that
	// were never processed (e.g. from a concurrent Reset that completed
	// after Wait started).
	r.mu.Lock()
	for _, seg := range r.tombstoneSegments {
		if err := seg.close(); err != nil && lastErr == nil {
			lastErr = err
		}
	}
	r.tombstoneSegments = nil
	r.mu.Unlock()

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
	revMap := *r.revMap.Load()
	lastID := revMap[lastIdx]

	if idx != lastIdx {
		// RCU-style: copy and swap
		newRev := make([]uint64, len(revMap)-1)
		copy(newRev, revMap[:idx])
		if idx < lastIdx {
			copy(newRev[idx:], revMap[idx+1:lastIdx])
		}
		newRev[idx] = lastID
		r.revMap.Store(&newRev)
		r.idMap[lastID] = idx

		srcSlot := r.getVectorSlice(int(lastIdx))
		dstSlot := r.getVectorSlice(int(idx))
		copy(dstSlot, srcSlot)
	} else {
		truncated := revMap[:lastIdx]
		r.revMap.Store(&truncated)
	}

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

func (r *VectorRegistry) FullDim() int {
	return r.config.FullDim
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
// BadgerDB has a 1MB value limit per key, so we cap at 900KB / vectorSize.
const snapshotChunkSize = 10000

func (r *VectorRegistry) SaveSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	numVectors := len(*r.revMap.Load())
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

	// Compute safe chunk size to stay under BadgerDB 1MB value limit.
	// Reserve headroom for key overhead.
	chunkSize := snapshotChunkSize
	if maxBytes := 900000 / r.vectorSize; maxBytes > 0 && maxBytes < chunkSize {
		chunkSize = maxBytes
	}

	batch := r.db.NewWriteBatch()
	defer batch.Cancel()

	// Save vectors in chunked keys to avoid a single giant allocation
	for start := 0; start < numVectors; start += chunkSize {
		end := start + chunkSize
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
		key := fmt.Appendf(nil, "sys:tq:vectors:%d", start/chunkSize)
		if err := batch.Set(key, buf); err != nil {
			return fmt.Errorf("failed to save TQ vector chunk %d: %w", start/chunkSize, err)
		}
	}

	// Save IDs in chunked keys
	revMap := *r.revMap.Load()
	for start := 0; start < numVectors; start += chunkSize {
		end := start + chunkSize
		if end > numVectors {
			end = numVectors
		}
		idsBytes := make([]byte, (end-start)*8)
		for i := start; i < end; i++ {
			binary.BigEndian.PutUint64(idsBytes[(i-start)*8:(i-start+1)*8], revMap[i])
		}
		key := fmt.Appendf(nil, "sys:tq:ids:%d", start/chunkSize)
		if err := batch.Set(key, idsBytes); err != nil {
			return fmt.Errorf("failed to save TQ id chunk %d: %w", start/chunkSize, err)
		}
	}

	// Save metadata: total count + vector size for validation on load
	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:], uint64(numVectors))
	binary.BigEndian.PutUint64(metaBuf[8:], uint64(r.vectorSize))
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

		// No snapshot — warm mmap cache from individual compressed vector keys
		slog.Info("no snapshot found, warming cache from Badger compressed vectors")
		return r.loadFromBadger(txn)
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
	var savedVectorSize uint64
	if err := metaItem.Value(func(val []byte) error {
		numTotal = binary.BigEndian.Uint64(val)
		if len(val) >= 16 {
			savedVectorSize = binary.BigEndian.Uint64(val[8:])
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to read snapshot meta: %w", err)
	}

	// Validate vector size matches current config (clean break format change)
	if savedVectorSize > 0 && uint64(r.vectorSize) != savedVectorSize {
		return fmt.Errorf("snapshot vector size mismatch: saved=%d, current=%d (clean break: recreate snapshot)", savedVectorSize, r.vectorSize)
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
	newRev := make([]uint64, numVectors)
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
				newRev[idIdx] = binary.BigEndian.Uint64(val[i*8 : (i+1)*8])
				idIdx++
			}
			return nil
		}); err != nil {
			return err
		}
	}

	r.revMap.Store(&newRev)

	r.idMap = make(map[uint64]uint32, numVectors)
	for idx, id := range newRev {
		r.idMap[id] = uint32(idx)
	}

	return nil
}

// loadFromBadger streams compressed vectors from individual Badger keys to warm the mmap cache.
// Used as fallback when no chunked snapshot exists.
func (r *VectorRegistry) loadFromBadger(txn *badger.Txn) error {
	itOpts := badger.DefaultIteratorOptions
	itOpts.PrefetchValues = true
	it := txn.NewIterator(itOpts)
	defer it.Close()

	prefix := []byte{keys.VectorFullPrefix}
	count := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()
		if len(key) != keys.ChunkKeySize {
			continue
		}

		id := binary.BigEndian.Uint64(key[1:])

		err := item.Value(func(val []byte) error {
			if len(val) < 1 {
				return nil
			}
			hashByte := val[0]
			vecData := val[1:]

			// Check if ID already exists
			if idx, exists := r.idMap[id]; exists {
				// Overwrite in mmap cache
				slot := r.getVectorSlice(int(idx))
				slot[0] = hashByte
				copy(slot[hashSize:hashSize+len(vecData)], vecData)
				return nil
			}

			// Append new vector
			idx := uint32(r.totalVectors)
			if err := r.ensureCapacity(r.totalVectors + 1); err != nil {
				return fmt.Errorf("failed to ensure capacity for vector %d: %w", r.totalVectors, err)
			}

			r.idMap[id] = idx
			r.appendRevMap(id)

			slot := r.getVectorSlice(int(idx))
			slot[0] = hashByte
			copy(slot[hashSize:hashSize+len(vecData)], vecData)

			r.totalVectors++
			return nil
		})
		if err != nil {
			return err
		}
		count++
	}

	if count > 0 {
		slog.Info("warmed mmap cache from Badger", "vectorCount", count)
	}
	return nil
}
