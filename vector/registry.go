package vector

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	FullDim         int
	TQBitWidth      int
	TQBlockSize     int
	NumWorkers      int
	VectorCapacity  int
	InitialCapacity int
}

func DefaultConfig() *Config {
	return &Config{
		FullDim:         1536,
		TQBitWidth:      8,
		TQBlockSize:     32,
		NumWorkers:      4,
		VectorCapacity:  25 * 1024 * 1024,
		InitialCapacity: 100000,
	}
}

func (c *Config) Validate() error {
	if c.FullDim <= 0 {
		return fmt.Errorf("FullDim must be positive")
	}
	if c.TQBitWidth != 4 && c.TQBitWidth != 8 {
		return fmt.Errorf("TQBitWidth must be 4 or 8")
	}
	if c.TQBlockSize <= 0 || c.TQBlockSize%8 != 0 {
		return fmt.Errorf("TQBlockSize must be positive and divisible by 8")
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

func (c *Config) TQConfig() *TurboQuantConfig {
	return &TurboQuantConfig{
		BitWidth:  c.TQBitWidth,
		BlockSize: c.TQBlockSize,
	}
}

// VectorRegistry holds TurboQuant compressed vectors in RAM for fast search.
// Vectors are compressed to 4-bit or 8-bit blockwise quantization with full 1536 dimensions.
// Full float32 vectors are persisted to BadgerDB for accurate retrieval.
type VectorRegistry struct {
	config     *Config
	tqConfig   *TurboQuantConfig
	vectorSize int

	data   []byte
	idMap  map[uint64]uint32
	revMap []uint64

	db *badger.DB
	mu sync.RWMutex
	wg sync.WaitGroup
}

func NewRegistry(db *badger.DB, cfg *Config) *VectorRegistry {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("invalid vector config: %v", err))
	}

	tqCfg := cfg.TQConfig()
	vSize := TQVectorSize(cfg.FullDim, tqCfg)

	return &VectorRegistry{
		config:     cfg,
		tqConfig:   tqCfg,
		vectorSize: vSize,
		data:       make([]byte, 0, cfg.VectorCapacity/vSize*vSize),
		idMap:      make(map[uint64]uint32, cfg.InitialCapacity),
		revMap:     make([]uint64, 0, cfg.InitialCapacity),
		db:         db,
	}
}

func (r *VectorRegistry) Add(id uint64, fullVec []float32) error {
	if len(fullVec) != r.config.FullDim {
		return fmt.Errorf("invalid vector dimension: expected %d, got %d", r.config.FullDim, len(fullVec))
	}

	tqData := QuantizeTurboQuant(fullVec, r.tqConfig)

	r.mu.Lock()
	if idx, exists := r.idMap[id]; exists {
		dstOffset := int(idx) * r.vectorSize
		copy(r.data[dstOffset:dstOffset+r.vectorSize], tqData)
		r.mu.Unlock()
	} else {
		idx := uint32(len(r.revMap))
		r.idMap[id] = idx
		r.revMap = append(r.revMap, id)
		r.data = append(r.data, tqData...)
		r.mu.Unlock()
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		key := make([]byte, 1+8)
		key[0] = 0x10
		binary.BigEndian.PutUint64(key[1:9], id)
		value := make([]byte, r.config.FullDim*4)
		for i, v := range fullVec {
			binary.LittleEndian.PutUint32(value[i*4:(i+1)*4], math.Float32bits(v))
		}
		_ = r.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
	}()

	return nil
}

func (r *VectorRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.revMap)
}

func (r *VectorRegistry) Close() error {
	r.wg.Wait()
	return nil
}

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

		srcOffset := int(lastIdx) * r.vectorSize
		dstOffset := int(idx) * r.vectorSize
		copy(r.data[dstOffset:dstOffset+r.vectorSize], r.data[srcOffset:srcOffset+r.vectorSize])
	}

	r.revMap = r.revMap[:lastIdx]
	delete(r.idMap, id)
	r.data = r.data[:len(r.data)-r.vectorSize]

	return true
}

func (r *VectorRegistry) HasVector(id uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.idMap[id]
	return exists
}

func (r *VectorRegistry) GetTQVector(idx int) []byte {
	if idx < 0 || idx >= len(r.revMap) {
		return nil
	}
	offset := idx * r.vectorSize
	return r.data[offset : offset+r.vectorSize]
}

func (r *VectorRegistry) TQConfig() *TurboQuantConfig {
	return r.tqConfig
}

func (r *VectorRegistry) VectorSize() int {
	return r.vectorSize
}

func (r *VectorRegistry) GetFullVector(id uint64) ([]float32, error) {
	key := make([]byte, 1+8)
	key[0] = 0x10
	binary.BigEndian.PutUint64(key[1:9], id)

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

func (r *VectorRegistry) SaveSnapshot() error {
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()

	numVectors := len(r.revMap)
	slog.Info("saving TQ vector snapshot",
		"vectorCount", numVectors,
		"dataSizeBytes", len(r.data),
	)

	idsBytes := make([]byte, len(r.revMap)*8)
	for i, id := range r.revMap {
		binary.BigEndian.PutUint64(idsBytes[i*8:(i+1)*8], id)
	}

	batch := r.db.NewWriteBatch()
	defer batch.Cancel()

	if err := batch.Set([]byte("sys:tq:vectors"), r.data); err != nil {
		return fmt.Errorf("failed to save TQ vectors: %w", err)
	}
	if err := batch.Set([]byte("sys:tq:ids"), idsBytes); err != nil {
		return fmt.Errorf("failed to save TQ ids: %w", err)
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	slog.Info("TQ vector snapshot saved", "vectorCount", numVectors)
	return nil
}

func (r *VectorRegistry) LoadSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	slog.Info("loading TQ vector snapshot")

	var vectorsBytes, idsBytes []byte

	err := r.db.View(func(txn *badger.Txn) error {
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

		return nil
	})

	if err != nil {
		return err
	}

	if vectorsBytes == nil || idsBytes == nil {
		return nil
	}

	numVectors := len(vectorsBytes) / r.vectorSize
	r.data = vectorsBytes

	r.revMap = make([]uint64, numVectors)
	for i := 0; i < numVectors; i++ {
		r.revMap[i] = binary.BigEndian.Uint64(idsBytes[i*8 : (i+1)*8])
	}

	r.idMap = make(map[uint64]uint32, numVectors)
	for idx, id := range r.revMap {
		r.idMap[id] = uint32(idx)
	}

	slog.Info("TQ vector snapshot loaded",
		"vectorCount", numVectors,
		"vectorSize", r.vectorSize,
	)

	return nil
}
