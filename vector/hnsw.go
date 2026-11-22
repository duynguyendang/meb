package vector

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

const (
	// DefaultHNSWCompactionRatio triggers incremental compaction when tombstones
	// reach 15% of active+deleted nodes in a topic.
	DefaultHNSWCompactionRatio = 0.15
	// DefaultHNSWCompactionBatchSize bounds nodes inspected per incremental compaction tick.
	DefaultHNSWCompactionBatchSize = 500
	hnswNodeLockStripes            = 256
)

type HNSWConfig struct {
	M                      int
	MMax                   int
	EfConstruction         int
	EfSearch               int
	ML                     float64
	CompactionRatio        float64 // Tombstone ratio threshold (default 0.15)
	CompactionBatchSize    int     // Nodes per incremental compaction batch (default 500)
}

func DefaultHNSWConfig() *HNSWConfig {
	return &HNSWConfig{
		M:                   16,
		MMax:                32,
		EfConstruction:      200,
		EfSearch:            64,
		ML:                  1.0 / math.Log(16.0),
		CompactionRatio:     DefaultHNSWCompactionRatio,
		CompactionBatchSize: DefaultHNSWCompactionBatchSize,
	}
}

func (c *HNSWConfig) Validate() error {
	if c.M < 1 {
		return fmt.Errorf("M must be >= 1, got %d", c.M)
	}
	if c.MMax < c.M {
		return fmt.Errorf("MMax must be >= M (%d), got %d", c.M, c.MMax)
	}
	if c.EfConstruction < 1 {
		return fmt.Errorf("EfConstruction must be >= 1, got %d", c.EfConstruction)
	}
	if c.EfSearch < 1 {
		return fmt.Errorf("EfSearch must be >= 1, got %d", c.EfSearch)
	}
	return nil
}

type HNSWIndex struct {
	db        *badger.DB
	cfg       *HNSWConfig
	fullDim   int
	paddedDim int
	rngMu     sync.Mutex
	rng       *rand.Rand

	entryPointMu sync.RWMutex
	entryPoints  map[uint32]uint64

	maxLevelMu sync.RWMutex
	maxLevels  map[uint32]int // per-topic top-level, cached to avoid Badger scan per descent

	nodeCountMu sync.RWMutex
	nodeCounts  map[uint32]int

	// Vector cache: avoids repeated Badger lookups during search.
	// Keyed by packedNodeID. Only used during search/insert sessions.
	vecCacheMu sync.RWMutex
	vecCache   map[uint64][]float32

	// Fine-grained striped node locks replace the coarse global graphMu so
	// concurrent inserts, reads, and incremental compaction only contend when
	// mutating the same node ID stripe.
	nodeLocks [hnswNodeLockStripes]sync.RWMutex

	// tombstones is an in-memory cache of soft-deleted node IDs.
	// Checked on every neighbor expansion in searchLayer to avoid returning
	// deleted nodes. Must be kept in sync with BadgerDB tombstone keys.
	tombstonesMu sync.RWMutex
	tombstones   map[uint64]struct{}

	// Per-topic atomic tombstone counters and incremental compaction cursors.
	tombstoneCountsMu sync.RWMutex
	tombstoneCounts   map[uint32]*atomic.Int64

	compactMu     sync.Mutex
	compactStates map[uint32]*hnswCompactState
}

func NewHNSWIndex(db *badger.DB, cfg *HNSWConfig, fullDim int) *HNSWIndex {
	paddedDim := nextPow2(fullDim)
	return &HNSWIndex{
		db:              db,
		cfg:             cfg,
		fullDim:         fullDim,
		paddedDim:       paddedDim,
		rng:             rand.New(rand.NewSource(rand.Int63())),
		entryPoints:     make(map[uint32]uint64),
		maxLevels:       make(map[uint32]int),
		nodeCounts:      make(map[uint32]int),
		vecCache:        make(map[uint64][]float32),
		tombstones:      make(map[uint64]struct{}),
		tombstoneCounts: make(map[uint32]*atomic.Int64),
		compactStates:   make(map[uint32]*hnswCompactState),
	}
}

func (idx *HNSWIndex) nodeStripe(packedID uint64) *sync.RWMutex {
	return &idx.nodeLocks[(packedID^(packedID>>17))&(hnswNodeLockStripes-1)]
}

func (idx *HNSWIndex) Insert(ctx context.Context, topicID uint32, localID uint64, fullVec []float32) error {
	transformed := PadAndTransform(fullVec, idx.paddedDim)
	packedID := keys.PackID(topicID, localID)

	// PackID(0, 0) == 0 which collides with the map zero-value sentinel.
	// This should never happen (topicID 0 is rejected by SetTopicID, localID
	// starts at 1 from the dict allocator), but guard defensively.
	if packedID == 0 {
		return fmt.Errorf("HNSW Insert: PackID(%d, %d) produced 0", topicID, localID)
	}

	// Persist raw float32 vector for future loadNodeVector calls (survives restart).
	rawKey := keys.EncodeIVFRawVectorKey(topicID, localID)
	rawBuf := make([]byte, idx.fullDim*4)
	for i := 0; i < idx.fullDim; i++ {
		binary.LittleEndian.PutUint32(rawBuf[i*4:], math.Float32bits(fullVec[i]))
	}
	if err := idx.db.Update(func(txn *badger.Txn) error {
		return txn.Set(rawKey, rawBuf)
	}); err != nil {
		return err
	}

	idx.vecCacheMu.Lock()
	idx.vecCache[packedID] = transformed
	idx.vecCacheMu.Unlock()

	level := idx.randomLevel()

	idx.entryPointMu.Lock()
	entryPoint, hasEP := idx.entryPoints[topicID]
	if !hasEP {
		// Write node before publishing entryPoint so concurrent searchers never observe an empty EP.
		if err := idx.writeNode(ctx, packedID, level, nil); err != nil {
			idx.entryPointMu.Unlock()
			return err
		}
		idx.entryPoints[topicID] = packedID
		idx.entryPointMu.Unlock()

		idx.maxLevelMu.Lock()
		idx.maxLevels[topicID] = level
		idx.maxLevelMu.Unlock()

		idx.nodeCountMu.Lock()
		idx.nodeCounts[topicID]++
		idx.nodeCountMu.Unlock()

		return nil
	}
	idx.entryPointMu.Unlock()

	currEntry := entryPoint
	if maxLevel, ok := idx.getMaxLevel(topicID); ok {
		for l := maxLevel; l > level; l-- {
			nearest := idx.searchLayer(transformed, currEntry, 1, l)
			if len(nearest) > 0 {
				currEntry = nearest[0].id
			}
		}
	}
	candidates := idx.searchLayer(transformed, currEntry, idx.cfg.EfConstruction, 0)
	neighborIDs := make([]uint64, len(candidates))
	for i, c := range candidates {
		neighborIDs[i] = c.id
	}
	neighbors := idx.selectNeighbors(neighborIDs, idx.cfg.M, level)

	if err := idx.writeNode(ctx, packedID, level, neighbors); err != nil {
		return err
	}

	idx.nodeCountMu.Lock()
	idx.nodeCounts[topicID]++
	idx.nodeCountMu.Unlock()

	for _, neighbor := range neighbors {
		if err := idx.addSymmetricalLink(ctx, neighbor, packedID, level); err != nil {
			slog.Warn("HNSW: failed to add symmetrical link",
				"node", packedID, "neighbor", neighbor, "level", level, "error", err)
		}
	}

	return nil
}

func (idx *HNSWIndex) randomLevel() int {
	idx.rngMu.Lock()
	r := idx.rng.Float64()
	idx.rngMu.Unlock()
	return int(math.Floor(-math.Log(r) * idx.cfg.ML))
}

func (idx *HNSWIndex) getEntryPoint(topicID uint32) (uint64, bool) {
	idx.entryPointMu.RLock()
	defer idx.entryPointMu.RUnlock()
	ep, ok := idx.entryPoints[topicID]
	return ep, ok
}

// getMaxLevel returns the cached top level for the topic's entry point.
// Avoids a full Badger prefix scan per descent step.
// Returns (0, false) if the topic has no cached max level.
func (idx *HNSWIndex) getMaxLevel(topicID uint32) (int, bool) {
	idx.maxLevelMu.RLock()
	defer idx.maxLevelMu.RUnlock()
	lvl, ok := idx.maxLevels[topicID]
	return lvl, ok
}

// loadNodeVector loads a node's transformed vector from cache or Badger.
// Returns an error if the node has been soft-deleted.
func (idx *HNSWIndex) loadNodeVector(packedNodeID uint64) ([]float32, error) {
	// Reject tombstoned nodes.
	idx.tombstonesMu.RLock()
	_, deleted := idx.tombstones[packedNodeID]
	idx.tombstonesMu.RUnlock()
	if deleted {
		return nil, fmt.Errorf("node %d is soft-deleted", packedNodeID)
	}

	idx.vecCacheMu.RLock()
	if vec, ok := idx.vecCache[packedNodeID]; ok {
		idx.vecCacheMu.RUnlock()
		return vec, nil
	}
	idx.vecCacheMu.RUnlock()

	topicID := keys.UnpackTopicID(packedNodeID)
	localID := keys.UnpackLocalID(packedNodeID)
	rawKey := keys.EncodeIVFRawVectorKey(topicID, localID)

	var rawVec []float32
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) < idx.fullDim*4 {
				return fmt.Errorf("raw vector too short: %d bytes", len(val))
			}
			rawVec = make([]float32, idx.fullDim)
			for i := 0; i < idx.fullDim; i++ {
				rawVec[i] = math.Float32frombits(binary.LittleEndian.Uint32(val[i*4:]))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	vec := PadAndTransform(rawVec, idx.paddedDim)

	idx.vecCacheMu.Lock()
	idx.vecCache[packedNodeID] = vec
	idx.vecCacheMu.Unlock()

	return vec, nil
}

// distanceTransformed computes squared L2 distance between a pre-loaded query
// vector and a node loaded from cache/Badger.
func (idx *HNSWIndex) distanceTransformed(queryVec []float32, packedNodeID uint64) float32 {
	vec, err := idx.loadNodeVector(packedNodeID)
	if err != nil {
		return math.MaxFloat32
	}

	dist := float32(0)
	for i := 0; i < idx.paddedDim; i++ {
		diff := queryVec[i] - vec[i]
		dist += diff * diff
	}
	return dist
}

// distanceBetween computes squared L2 distance between two nodes.
func (idx *HNSWIndex) distanceBetween(id1, id2 uint64) float32 {
	vec1, err := idx.loadNodeVector(id1)
	if err != nil {
		return math.MaxFloat32
	}
	vec2, err := idx.loadNodeVector(id2)
	if err != nil {
		return math.MaxFloat32
	}

	dist := float32(0)
	for i := 0; i < idx.paddedDim; i++ {
		diff := vec1[i] - vec2[i]
		dist += diff * diff
	}
	return dist
}

func (idx *HNSWIndex) searchLayer(queryVec []float32, entryPoint uint64, ef int, level int) []nodeDist {
	// If entry point is tombstoned, return empty — no valid starting point.
	idx.tombstonesMu.RLock()
	_, deleted := idx.tombstones[entryPoint]
	idx.tombstonesMu.RUnlock()
	if deleted {
		return nil
	}

	epDist := idx.distanceTransformed(queryVec, entryPoint)

	candidates := newMinHeap2(ef * 2)
	candidates.push(nodeDist{id: entryPoint, dist: epDist})

	results := newMaxHeap(ef)
	results.push(nodeDist{id: entryPoint, dist: epDist})

	visited := make(map[uint64]bool)
	visited[entryPoint] = true

	for candidates.Len() > 0 {
		c := candidates.pop()

		if results.Len() >= ef {
			farthest := results.peek()
			if c.dist > farthest.dist {
				break
			}
		}

		neighbors, err := idx.readNeighbors(c.id, level)
		if err != nil {
			continue
		}

		for _, nid := range neighbors {
			if visited[nid] {
				continue
			}
			visited[nid] = true

			// Skip soft-deleted nodes (tombstoned between SoftDelete and Compact).
			idx.tombstonesMu.RLock()
			_, deleted := idx.tombstones[nid]
			idx.tombstonesMu.RUnlock()
			if deleted {
				continue
			}

			nd := nodeDist{id: nid, dist: idx.distanceTransformed(queryVec, nid)}

			farthest := results.peek()
			if nd.dist < farthest.dist || results.Len() < ef {
				candidates.push(nd)
				results.push(nd)
				if results.Len() > ef {
					results.pop()
				}
			}
		}
	}

	return results.drain()
}

func (idx *HNSWIndex) SearchInTopic(ctx context.Context, topicID uint32, queryVec []float32, k int) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		transformed := PadAndTransform(queryVec, idx.paddedDim)

		entryPoint, hasEP := idx.getEntryPoint(topicID)
		if !hasEP {
			slog.Debug("HNSW: no entry point")
			return
		}

		maxLevel, hasLevel := idx.getMaxLevel(topicID)
		slog.Debug("HNSW entry", "entryPoint", entryPoint, "maxLevel", maxLevel)
		currEntry := entryPoint
		if hasLevel {
			for l := maxLevel; l > 0; l-- {
				nearest := idx.searchLayer(transformed, currEntry, 1, l)
				slog.Debug("HNSW level", "level", l, "nearestCount", len(nearest))
				if len(nearest) > 0 {
					currEntry = nearest[0].id
				}
			}
		}

		ef := idx.cfg.EfSearch
		if k > ef {
			ef = k
		}
		candidates := idx.searchLayer(transformed, currEntry, ef, 0)
		slog.Debug("HNSW candidates", "count", len(candidates))

		count := 0
		for _, c := range candidates {
			if count >= k {
				break
			}
			if err := ctx.Err(); err != nil {
				yield(SearchResult{}, err)
				return
			}
			score := 1.0 / (1.0 + c.dist)
			if !yield(SearchResult{
				ID:    c.id,
				Score: score,
			}, nil) {
				return
			}
			count++
		}
	}
}

func (idx *HNSWIndex) addSymmetricalLink(ctx context.Context, existingID uint64, newNodeID uint64, level int) error {
	lock := idx.nodeStripe(existingID)
	lock.Lock()
	defer lock.Unlock()

	neighbors, err := idx.readNeighborsUnlocked(existingID, level)
	if err != nil {
		neighbors = nil
	}

	maxConn := idx.cfg.M
	if level == 0 {
		maxConn = idx.cfg.MMax
	}
	if len(neighbors) < maxConn {
		neighbors = append(neighbors, newNodeID)
	} else {
		newNodeDist := idx.distanceBetween(existingID, newNodeID)
		farthestIdx := -1
		farthestDist := float32(0)
		for i, nid := range neighbors {
			d := idx.distanceBetween(existingID, nid)
			// Skip nodes that failed to load (returns MaxFloat32).
			if d >= math.MaxFloat32-1 {
				continue
			}
			if d > farthestDist {
				farthestDist = d
				farthestIdx = i
			}
		}
		if farthestIdx >= 0 && newNodeDist < farthestDist {
			neighbors[farthestIdx] = newNodeID
		}
	}

	return idx.writeNodeUnlocked(ctx, existingID, level, neighbors)
}

func (idx *HNSWIndex) selectNeighbors(candidates []uint64, m int, level int) []uint64 {
	if len(candidates) <= m {
		return candidates
	}
	return candidates[:m]
}

func (idx *HNSWIndex) GetEntryPoint(topicID uint32) (uint64, bool) {
	return idx.getEntryPoint(topicID)
}

func (idx *HNSWIndex) GetMaxLevel(topicID uint32) (int, bool) {
	return idx.getMaxLevel(topicID)
}

func (idx *HNSWIndex) ReadLevel(packedID uint64) (int, error) {
	return idx.readLevel(packedID)
}

func (idx *HNSWIndex) ReadNeighbors(packedID uint64, level int) ([]uint64, error) {
	return idx.readNeighbors(packedID, level)
}
