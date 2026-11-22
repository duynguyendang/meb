package vector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

// IndexState represents the lifecycle state of a topic's vector index.
type IndexState uint32

const (
	// StateBruteForce serves queries via parallel mmap scan with Cauchy-Schwarz pruning.
	StateBruteForce IndexState = iota
	// StateBuilding trains and populates the ANN index asynchronously while continuing
	// to serve reads from mmap and buffer concurrent writes in delta.
	StateBuilding
	// StateANN routes topic queries to the promoted ANN index (atomic pointer swap).
	StateANN
)

func (s IndexState) String() string {
	switch s {
	case StateBruteForce:
		return "BruteForce"
	case StateBuilding:
		return "Building"
	case StateANN:
		return "ANN"
	default:
		return fmt.Sprintf("Unknown(%d)", uint32(s))
	}
}

// DefaultAutoPromoteThreshold is the default per-topic vector count at which
// VectorRegistry automatically promotes BruteForce scans to an ANN index.
const DefaultAutoPromoteThreshold = 50_000

type deltaEntry struct {
	localID uint64
	fullVec []float32
	deleted bool
}

type topicIndexState struct {
	topicID     uint32
	state       atomic.Uint32 // IndexState
	vectorCount atomic.Int64
	annType     ANNIndexType

	ivfpq atomic.Pointer[IVFPQIndex]
	hnsw  atomic.Pointer[HNSWIndex]

	deltaMu sync.Mutex
	delta   []deltaEntry
}

// TopicIndexState returns the current adaptive lifecycle state for topicID.
func (r *VectorRegistry) TopicIndexState(topicID uint32) IndexState {
	r.adaptiveMu.RLock()
	ts := r.topicStates[topicID]
	r.adaptiveMu.RUnlock()
	if ts == nil {
		return StateBruteForce
	}
	return IndexState(ts.state.Load())
}

// TopicVectorCount returns the tracked active vector count for topicID.
func (r *VectorRegistry) TopicVectorCount(topicID uint32) int64 {
	r.adaptiveMu.RLock()
	ts := r.topicStates[topicID]
	r.adaptiveMu.RUnlock()
	if ts == nil {
		return 0
	}
	return ts.vectorCount.Load()
}

// PromotedIVFPQIndex returns the auto-promoted IVF-PQ index for topicID, if active.
func (r *VectorRegistry) PromotedIVFPQIndex(topicID uint32) *IVFPQIndex {
	r.adaptiveMu.RLock()
	ts := r.topicStates[topicID]
	r.adaptiveMu.RUnlock()
	if ts == nil || IndexState(ts.state.Load()) != StateANN {
		return nil
	}
	return ts.ivfpq.Load()
}

// WaitForAutoPromote blocks until all background index promotions have completed.
func (r *VectorRegistry) WaitForAutoPromote() {
	r.buildWG.Wait()
}

func (r *VectorRegistry) getOrCreateTopicState(topicID uint32) *topicIndexState {
	r.adaptiveMu.RLock()
	ts := r.topicStates[topicID]
	r.adaptiveMu.RUnlock()
	if ts != nil {
		return ts
	}

	r.adaptiveMu.Lock()
	defer r.adaptiveMu.Unlock()
	if ts = r.topicStates[topicID]; ts != nil {
		return ts
	}
	ts = &topicIndexState{
		topicID: topicID,
		annType: r.autoPromoteANNType(),
	}
	r.topicStates[topicID] = ts
	return ts
}

func (r *VectorRegistry) autoPromoteThreshold() int {
	if r.config.AutoPromoteThreshold < 0 {
		return 0 // explicitly disabled
	}
	if r.config.AutoPromoteThreshold == 0 {
		return DefaultAutoPromoteThreshold
	}
	return r.config.AutoPromoteThreshold
}

func (r *VectorRegistry) autoPromoteANNType() ANNIndexType {
	if r.config.AutoPromoteANNType == IndexHNSW {
		return IndexHNSW
	}
	return IndexIVFPQ
}

func (r *VectorRegistry) resetBuildContext() {
	r.buildCancelMu.Lock()
	if r.buildCancel != nil {
		r.buildCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.buildCtx = ctx
	r.buildCancel = cancel
	r.buildCancelMu.Unlock()
}

func (r *VectorRegistry) currentBuildContext() context.Context {
	r.buildCancelMu.Lock()
	ctx := r.buildCtx
	r.buildCancelMu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// recordVectorAdd updates per-topic counts, records concurrent writes in delta
// during StateBuilding, writes through to StateANN, or triggers StateBruteForce -> StateBuilding.
func (r *VectorRegistry) recordVectorAdd(id uint64, fullVec []float32, isNew bool) {
	topicID := keys.UnpackTopicID(id)
	localID := keys.UnpackLocalID(id)
	ts := r.getOrCreateTopicState(topicID)

	var count int64
	if isNew {
		count = ts.vectorCount.Add(1)
	} else {
		count = ts.vectorCount.Load()
	}

	state := IndexState(ts.state.Load())
	if state == StateBruteForce {
		thresh := r.autoPromoteThreshold()
		if thresh > 0 && count >= int64(thresh) && !r.closed.Load() {
			if ts.state.CompareAndSwap(uint32(StateBruteForce), uint32(StateBuilding)) {
				r.buildWG.Add(1)
				go r.buildTopicANN(r.currentBuildContext(), ts)
				// Fall through to capture any write triggered after CAS if fullVec != nil.
			}
		}
		return
	}

	if fullVec == nil {
		return
	}

	ts.deltaMu.Lock()
	switch IndexState(ts.state.Load()) {
	case StateBuilding:
		vecCopy := make([]float32, len(fullVec))
		copy(vecCopy, fullVec)
		ts.delta = append(ts.delta, deltaEntry{
			localID: localID,
			fullVec: vecCopy,
		})
	case StateANN:
		if idx := ts.ivfpq.Load(); idx != nil {
			_ = r.db.Update(func(txn *badger.Txn) error {
				return idx.AddVector(txn, topicID, localID, fullVec)
			})
		} else if idx := ts.hnsw.Load(); idx != nil {
			_ = idx.Insert(context.Background(), topicID, localID, fullVec)
		}
	}
	ts.deltaMu.Unlock()
}

func (r *VectorRegistry) recordVectorDelete(id uint64) {
	topicID := keys.UnpackTopicID(id)
	localID := keys.UnpackLocalID(id)

	r.adaptiveMu.RLock()
	ts := r.topicStates[topicID]
	r.adaptiveMu.RUnlock()
	if ts == nil {
		return
	}

	for {
		cur := ts.vectorCount.Load()
		if cur <= 0 || ts.vectorCount.CompareAndSwap(cur, cur-1) {
			break
		}
	}

	ts.deltaMu.Lock()
	switch IndexState(ts.state.Load()) {
	case StateBuilding:
		ts.delta = append(ts.delta, deltaEntry{
			localID: localID,
			deleted: true,
		})
	case StateANN:
		if idx := ts.hnsw.Load(); idx != nil {
			_ = idx.SoftDelete(id)
		}
	}
	ts.deltaMu.Unlock()
}

// buildTopicANN trains and populates the topic's ANN index in the background,
// replays buffered delta mutations, and atomically transitions StateBuilding -> StateANN.
func (r *VectorRegistry) buildTopicANN(ctx context.Context, ts *topicIndexState) {
	defer r.buildWG.Done()

	rollback := func(reason string, err error) {
		ts.deltaMu.Lock()
		ts.delta = nil
		ts.state.CompareAndSwap(uint32(StateBuilding), uint32(StateBruteForce))
		ts.deltaMu.Unlock()
		if err != nil {
			slog.Warn("adaptive ANN build aborted, reverting to BruteForce",
				"topicID", ts.topicID, "reason", reason, "error", err)
		}
	}

	if ctx.Err() != nil || r.closed.Load() {
		rollback("cancelled", nil)
		return
	}

	count := ts.vectorCount.Load()
	annType := ts.annType

	slog.Info("adaptive vector index promotion started",
		"topicID", ts.topicID,
		"annType", annType,
		"vectorCount", count,
	)

	ivfCfg := autoIVFPQConfig(r.config.FullDim, count, r.hybridCfg)
	builtIdx := NewIVFPQIndex(r.db, ivfCfg, r.config.FullDim)

	if err := builtIdx.Train(ts.topicID, 0); err != nil {
		rollback("train_failed", err)
		return
	}

	if ctx.Err() != nil || r.closed.Load() {
		rollback("cancelled_after_train", nil)
		return
	}

	// Atomically replay buffered delta mutations and swap the active pointer.
	ts.deltaMu.Lock()
	defer ts.deltaMu.Unlock()

	if ctx.Err() != nil || r.closed.Load() {
		ts.delta = nil
		ts.state.CompareAndSwap(uint32(StateBuilding), uint32(StateBruteForce))
		return
	}

	deltaLen := len(ts.delta)
	if deltaLen > 0 {
		const deltaBatchSize = 2000
		for start := 0; start < deltaLen; start += deltaBatchSize {
			end := start + deltaBatchSize
			if end > deltaLen {
				end = deltaLen
			}
			err := r.db.Update(func(txn *badger.Txn) error {
				for _, entry := range ts.delta[start:end] {
					if entry.deleted {
						continue
					}
					if err := builtIdx.AddVector(txn, ts.topicID, entry.localID, entry.fullVec); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				ts.delta = nil
				ts.state.CompareAndSwap(uint32(StateBuilding), uint32(StateBruteForce))
				slog.Warn("adaptive ANN delta replay failed, reverting to BruteForce",
					"topicID", ts.topicID, "error", err)
				return
			}
		}
		ts.delta = nil
	}

	ts.ivfpq.Store(builtIdx)
	ts.state.Store(uint32(StateANN))

	slog.Info("adaptive vector index promoted to StateANN",
		"topicID", ts.topicID,
		"annType", annType,
		"centroids", ivfCfg.NumCentroids,
		"nprobe", ivfCfg.NProbe,
		"deltaReplayed", deltaLen,
	)

	if cb := r.config.OnAutoPromote; cb != nil {
		cb(ts.topicID, annType, count, deltaLen)
	}
}

func autoIVFPQConfig(fullDim int, corpusCount int64, hybridCfg *HybridConfig) *IVFPQConfig {
	paddedDim := nextPow2(fullDim)

	numSubSpaces := 64
	for numSubSpaces > 1 && paddedDim/numSubSpaces < 2 {
		numSubSpaces /= 2
	}

	numCentroids := int(math.Sqrt(float64(max(corpusCount, int64(1)))))
	if numCentroids < 4 {
		numCentroids = 4
	}
	if numCentroids > 1024 {
		numCentroids = 1024
	}

	nprobe := numCentroids / 4
	if nprobe < 1 {
		nprobe = 1
	}
	if nprobe > 32 {
		nprobe = 32
	}

	return &IVFPQConfig{
		NumCentroids:    numCentroids,
		NumSubSpaces:    numSubSpaces,
		BitsPerSpace:    8,
		NProbe:          nprobe,
		NSample:         100_000,
		BatchSize:       10_000,
		StoreRawVectors: false,
		HybridCfg:       hybridCfg,
	}
}
