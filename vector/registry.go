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

	segments          *atomic.Pointer[[]*mmapSegment]
	vectorsPerSegment int
	totalVectors      atomic.Int64

	// cellsPtr is an RCU-published slice of per-slot atomic pointers.
	// Each cell is independently swappable: writers allocate new slotData
	// and atomically store the pointer; readers load and read through it.
	// Data is immutable after publication. nil = uninitialized/tombstone.
	cellsPtr *atomic.Pointer[[]atomic.Pointer[slotData]]

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

// slotData holds the bytes for one vector slot.
// Once published via an atomic.Pointer[slotData] cell, data must not be mutated.
//
// Two backing stores exist: newly-appended slots point directly into the
// contiguous mmap region (zero-copy, no heap pressure). Overwrites and
// delete-swaps allocate a new slotData on the Go heap and atomically
// publish the pointer, detaching that slot from the mmap. For update-heavy
// or churn-heavy workloads this adds GC pressure and RSS beyond the mmap
// size, trading the "O(k) memory, disk-scaled" property for correctness.
// The primary store (BadgerDB) holds the authoritative copy regardless.
type slotData struct {
	data []byte
}

// appendRevMap extends the revMap by one entry.
// Allocates a new slice every time to avoid data races: in-place append
// mutates the slice header (len/cap) which a concurrent RLock reader
// might be mid-read on, seeing a truncated or extended view.
// Must be called under mu.Lock().
func (r *VectorRegistry) appendRevMap(id uint64) {
	oldRev := *r.revMap.Load()
	newRev := make([]uint64, len(oldRev)+1, max(len(oldRev)*2, 1))
	copy(newRev, oldRev)
	newRev[len(oldRev)] = id
	r.revMap.Store(&newRev)
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
		segments: func() *atomic.Pointer[[]*mmapSegment] {
			p := &atomic.Pointer[[]*mmapSegment]{}
			p.Store(&[]*mmapSegment{})
			return p
		}(),
		cellsPtr: func() *atomic.Pointer[[]atomic.Pointer[slotData]] {
			p := &atomic.Pointer[[]atomic.Pointer[slotData]]{}
			cells := make([]atomic.Pointer[slotData], 0, cfg.VectorCapacity)
			p.Store(&cells)
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

	oldSegs := *r.segments.Load()

	slog.Info("resetting vector registry",
		"totalVectors", r.totalVectors.Load(),
		"segments", len(oldSegs),
	)

	// Move current segments to tombstone list
	if len(oldSegs) > 0 {
		r.tombstoneSegments = append(r.tombstoneSegments, oldSegs...)
	}

	// Clear mappings
	r.idMap = make(map[uint64]uint32, r.config.InitialCapacity)
	r.revMap.Store(&[]uint64{})
	r.totalVectors.Store(0)
	r.segments.Store(&[]*mmapSegment{})
	r.cellsPtr.Store(&[]atomic.Pointer[slotData]{})

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
		"tombstonedSegments", len(r.tombstoneSegments),
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
	for {
		oldSegs := *r.segments.Load()
		if len(oldSegs) >= requiredSegs {
			return nil
		}
		segIdx := len(oldSegs)
		segBytes := r.vectorsPerSegment * r.vectorSize
		seg, err := newMmapSegment(r.config.SegmentDir, segIdx, segBytes)
		if err != nil {
			return fmt.Errorf("failed to create segment %d: %w", segIdx, err)
		}
		newSegs := make([]*mmapSegment, len(oldSegs)+1)
		copy(newSegs, oldSegs)
		newSegs[len(oldSegs)] = seg
		r.segments.Store(&newSegs)
	}
}

// ensureCells grows the cells slice so it has room for at least n entries.
// Must be called under mu.Lock().
func (r *VectorRegistry) ensureCells(n int) {
	old := *r.cellsPtr.Load()
	if len(old) >= n {
		return
	}
	newCap := max(n, len(old)*3/2)
	new := make([]atomic.Pointer[slotData], newCap)
	copy(new, old)
	r.cellsPtr.Store(&new)
}

// slotDataOf returns the slotData pointer for the given index, or nil if
// tombstoned/uninitialized. Safe for lock-free readers after a successful
// acquire-load of r.totalVectors (which orders the cellsPtr.Store before it).
//
// During a delete-swap a lock-free reader may briefly pair a moved slot's
// bytes with a stale revMap id — this is eventual consistency of search
// results, not a data race, and is acceptable for a cache layer.
func (r *VectorRegistry) slotDataOf(idx int) *slotData {
	return (*r.cellsPtr.Load())[idx].Load()
}

// slotBytes returns the full slot data (hash byte followed by vector data) for the
// given index, or nil if tombstoned. Safe for lock-free readers.
func (r *VectorRegistry) slotBytes(idx int) []byte {
	sd := r.slotDataOf(idx)
	if sd == nil {
		return nil
	}
	return sd.data
}

// getVectorSlice returns a writable slice into the underlying mmap for the vector
// at the given index. Safe only under r.mu.Lock() — used during initial append to
// write directly to the mmap before the slotData is published via the cell.
// For existing/published slots, use slotDataOf.
func (r *VectorRegistry) getVectorSlice(idx int) []byte {
	segs := *r.segments.Load()
	segIdx := idx / r.vectorsPerSegment
	offset := (idx % r.vectorsPerSegment) * r.vectorSize
	return segs[segIdx].data[offset : offset+r.vectorSize]
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
		// COW: allocate new buffer, write, atomically publish to the cell.
		buf := make([]byte, r.vectorSize)
		buf[0] = semanticHash
		copy(buf[hashSize:hashSize+hybridSize], hybridData)
		(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: buf})
		return nil
	}

	newTotal := r.totalVectors.Load() + 1
	if err := r.ensureCapacity(int(newTotal)); err != nil {
		return err
	}
	r.ensureCells(int(newTotal))

	idx := uint32(r.totalVectors.Load())
	r.idMap[id] = idx
	r.appendRevMap(id)

	slot := r.getVectorSlice(int(idx))
	slot[0] = semanticHash
	copy(slot[hashSize:hashSize+hybridSize], hybridData)

	// Publish the slot data and bump totalVectors atomically.
	(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: slot[:r.vectorSize]})
	r.totalVectors.Store(newTotal)
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
		// COW: allocate new buffer, write, atomically publish.
		buf := make([]byte, r.vectorSize)
		buf[0] = semanticHash
		copy(buf[hashSize:hashSize+hybridSize], hybridData)
		(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: buf})
		return nil
	}

	newTotal := r.totalVectors.Load() + 1
	if err := r.ensureCapacity(int(newTotal)); err != nil {
		return err
	}
	r.ensureCells(int(newTotal))

	idx := uint32(r.totalVectors.Load())
	r.idMap[id] = idx
	r.appendRevMap(id)

	slot := r.getVectorSlice(int(idx))
	slot[0] = semanticHash
	copy(slot[hashSize:hashSize+hybridSize], hybridData)

	// Publish the slot data and bump totalVectors atomically.
	(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: slot[:r.vectorSize]})
	r.totalVectors.Store(newTotal)
	return nil
}

func (r *VectorRegistry) Count() int {
	return len(*r.revMap.Load())
}

func (r *VectorRegistry) Close() error {
	r.closed.Store(true)

	// Close current segments without holding the lock during IO.
	r.mu.Lock()
	segments := *r.segments.Load()
	r.segments.Store(&[]*mmapSegment{})
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

	lastIdx := uint32(r.totalVectors.Load() - 1)
	revMap := *r.revMap.Load()
	lastID := revMap[lastIdx]

	if idx != lastIdx {
		// RCU-style revMap copy-and-swap
		newRev := make([]uint64, len(revMap)-1)
		copy(newRev, revMap[:idx])
		if idx < lastIdx {
			copy(newRev[idx:], revMap[idx+1:lastIdx])
		}
		newRev[idx] = lastID
		r.revMap.Store(&newRev)
		r.idMap[lastID] = idx

		// Pointer swap instead of byte copy: the hole cell gets the last
		// slot's slotData pointer; the last cell is tombstoned (nil).
		// No mmap byte is touched — readers see either old or new data,
		// both valid and immutable.
		cells := *r.cellsPtr.Load()
		lastData := cells[int(lastIdx)].Load()
		cells[int(idx)].Store(lastData)
		cells[int(lastIdx)].Store(nil)
	} else {
		truncated := revMap[:lastIdx]
		r.revMap.Store(&truncated)
		// Tombstone the cell for the removed slot.
		(*r.cellsPtr.Load())[int(idx)].Store(nil)
	}

	delete(r.idMap, id)
	r.totalVectors.Add(-1)

	return true
}

func (r *VectorRegistry) HasVector(id uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.idMap[id]
	return exists
}

// MarkSnapshotDirty sets the snapshot dirty flag in the given transaction.
// Called when a vector is deleted from BadgerDB to ensure the next LoadSnapshot
// falls back to authoritative Badger keys instead of a stale chunked snapshot.
func (r *VectorRegistry) MarkSnapshotDirty(txn *badger.Txn) error {
	return txn.Set(keys.KeySnapshotDirty, []byte{1})
}

// GetTQVector returns the TQ-compressed vector data at the given index (skipping hash byte).
// Lock-free safe: uses atomic load of totalVectors and per-slot cell pointer.
// The returned slice is immutable (data is never mutated after publication) and
// valid until the calling goroutine drops the reference — no copy needed.
func (r *VectorRegistry) GetTQVector(idx int) []byte {
	if idx < 0 || int64(idx) >= r.totalVectors.Load() {
		return nil
	}
	sd := r.slotDataOf(idx)
	if sd == nil {
		return nil
	}
	return sd.data[hashSize:]
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

// VectorCapacity returns the configured maximum vector capacity.
func (r *VectorRegistry) VectorCapacity() int {
	return r.config.VectorCapacity
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
		"segments", len(*r.segments.Load()),
	)

	for _, seg := range *r.segments.Load() {
		if err := seg.sync(); err != nil {
			slog.Warn("segment sync failed", "error", err)
		}
	}

	chunkSize := snapshotChunkSize
	if maxBytes := 900000 / r.vectorSize; maxBytes > 0 && maxBytes < chunkSize {
		chunkSize = maxBytes
	}

	batch := r.db.NewWriteBatch()
	defer batch.Cancel()

	for start := 0; start < numVectors; start += chunkSize {
		end := start + chunkSize
		if end > numVectors {
			end = numVectors
		}
		chunkBytes := (end - start) * r.vectorSize
		buf := make([]byte, chunkBytes)
		for i := start; i < end; i++ {
			sd := r.slotDataOf(i)
			if sd == nil {
				continue
			}
			off := (i - start) * r.vectorSize
			copy(buf[off:off+r.vectorSize], sd.data)
		}
		key := keys.EncodeSnapshotVectorChunkKey(start / chunkSize)
		if err := batch.Set(key, buf); err != nil {
			return fmt.Errorf("failed to save TQ vector chunk %d: %w", start/chunkSize, err)
		}
	}

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
		key := keys.EncodeSnapshotIDChunkKey(start / chunkSize)
		if err := batch.Set(key, idsBytes); err != nil {
			return fmt.Errorf("failed to save TQ id chunk %d: %w", start/chunkSize, err)
		}
	}

	metaBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(metaBuf[0:], uint64(numVectors))
	binary.BigEndian.PutUint64(metaBuf[8:], uint64(r.vectorSize))
	if err := batch.Set(keys.KeySnapshotMeta, metaBuf); err != nil {
		return fmt.Errorf("failed to save TQ snapshot meta: %w", err)
	}

	if err := batch.Delete(keys.KeySnapshotDirty); err != nil {
		return fmt.Errorf("failed to clear snapshot dirty flag: %w", err)
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	r.db.Update(func(txn *badger.Txn) error {
		txn.Delete([]byte("sys:tq:vectors"))
		txn.Delete([]byte("sys:tq:ids"))
		txn.Delete([]byte("sys:tq:meta"))
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
		_, err := txn.Get(keys.KeySnapshotDirty)
		if err == nil {
			slog.Info("snapshot dirty flag set, skipping snapshot (using authoritative Badger keys)")
			return r.loadFromBadger(txn)
		}
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to check snapshot dirty flag: %w", err)
		}

		metaItem, err := txn.Get(keys.KeySnapshotMeta)
		if err == nil {
			return r.loadChunkedSnapshot(txn, metaItem)
		}
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to load TQ snapshot meta: %w", err)
		}

		_, err = txn.Get([]byte("sys:tq:meta"))
		if err == nil {
			slog.Info("found legacy string-key snapshot, loading")
			return r.loadLegacyChunkedSnapshot(txn)
		}

		slog.Info("no snapshot found, warming cache from Badger compressed vectors")
		return r.loadFromBadger(txn)
	})

	if err != nil {
		return err
	}

	if r.totalVectors.Load() > 0 {
		slog.Info("TQ vector snapshot loaded",
			"vectorCount", r.totalVectors.Load(),
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
	r.ensureCells(numVectors)

	vecIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := keys.EncodeSnapshotVectorChunkKey(chunkIdx)
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
				(*r.cellsPtr.Load())[vecIdx].Store(&slotData{data: slot[:r.vectorSize]})
				vecIdx++
			}
			return nil
		}); err != nil {
			return err
		}
	}
	r.totalVectors.Store(int64(numVectors))

	newRev := make([]uint64, numVectors)
	idIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := keys.EncodeSnapshotIDChunkKey(chunkIdx)
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

func (r *VectorRegistry) loadLegacyChunkedSnapshot(txn *badger.Txn) error {
	metaItem, err := txn.Get([]byte("sys:tq:meta"))
	if err != nil {
		return fmt.Errorf("failed to load legacy snapshot meta: %w", err)
	}

	var numTotal uint64
	var savedVectorSize uint64
	if err := metaItem.Value(func(val []byte) error {
		numTotal = binary.BigEndian.Uint64(val)
		if len(val) >= 16 {
			savedVectorSize = binary.BigEndian.Uint64(val[8:])
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to read legacy snapshot meta: %w", err)
	}

	if savedVectorSize > 0 && uint64(r.vectorSize) != savedVectorSize {
		return fmt.Errorf("legacy snapshot vector size mismatch: saved=%d, current=%d", savedVectorSize, r.vectorSize)
	}

	numVectors := int(numTotal)
	if numVectors == 0 {
		return nil
	}

	if err := r.ensureCapacity(numVectors); err != nil {
		return fmt.Errorf("failed to ensure capacity for legacy snapshot: %w", err)
	}
	r.ensureCells(numVectors)

	vecIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := fmt.Appendf(nil, "sys:tq:vectors:%d", chunkIdx)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to load legacy TQ vector chunk %d: %w", chunkIdx, err)
		}
		if err := item.Value(func(val []byte) error {
			chunkVecs := len(val) / r.vectorSize
			for i := 0; i < chunkVecs; i++ {
				slot := r.getVectorSlice(vecIdx)
				copy(slot, val[i*r.vectorSize:(i+1)*r.vectorSize])
				(*r.cellsPtr.Load())[vecIdx].Store(&slotData{data: slot[:r.vectorSize]})
				vecIdx++
			}
			return nil
		}); err != nil {
			return err
		}
	}
	r.totalVectors.Store(int64(numVectors))

	newRev := make([]uint64, numVectors)
	idIdx := 0
	for chunkIdx := 0; ; chunkIdx++ {
		key := fmt.Appendf(nil, "sys:tq:ids:%d", chunkIdx)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to load legacy TQ id chunk %d: %w", chunkIdx, err)
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
				// COW: allocate new buffer, write, atomically publish.
				buf := make([]byte, hashSize+len(vecData))
				buf[0] = hashByte
				copy(buf[hashSize:], vecData)
				(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: buf})
				return nil
			}

			// Append new vector
			tv := r.totalVectors.Load()
			idx := uint32(tv)
			if err := r.ensureCapacity(int(tv + 1)); err != nil {
				return fmt.Errorf("failed to ensure capacity for vector %d: %w", tv, err)
			}
			r.ensureCells(int(tv + 1))

			r.idMap[id] = idx
			r.appendRevMap(id)

			slot := r.getVectorSlice(int(idx))
			slot[0] = hashByte
			copy(slot[hashSize:hashSize+len(vecData)], vecData)

			(*r.cellsPtr.Load())[int(idx)].Store(&slotData{data: slot[:r.vectorSize]})
			r.totalVectors.Store(tv + 1)
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
