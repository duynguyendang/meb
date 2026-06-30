package query

import (
	"context"
	"encoding/binary"
	"iter"
	"log/slog"
	"math"
	"sort"

	"github.com/duynguyendang/meb/keys"
	"github.com/dgraph-io/badger/v4"
)

const (
	// maxSampleKeys is the maximum number of keys to sample per relation
	// when estimating cardinality. Beyond this, we extrapolate from the sample.
	maxSampleKeys = 1000
)

// ExecuteWithSeeds runs an LFTJ join seeded with candidate IDs from vector search.
// This is the Neuro-First pipeline: TurboQuant seeds -> LFTJ join -> results.
// The seeds are streamed via iter.Seq2 — no intermediate slice allocation.
func (e *LFTJEngine) ExecuteWithSeeds(
	ctx context.Context,
	relations []RelationPattern,
	seedVar string,
	seeds iter.Seq2[uint64, error],
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	return func(yield func(map[string]uint64, error) bool) {
		for seedID, err := range seeds {
			if err != nil {
				yield(nil, err)
				return
			}

			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			boundVars := map[string]uint64{seedVar: seedID}

			for result, err := range e.Execute(ctx, relations, boundVars, resultVars) {
				if err != nil {
					yield(nil, err)
					return
				}
				if !yield(result, nil) {
					return
				}
			}
		}
	}
}

// RunQuery is a convenience method for running a query with bound variables.
func RunQuery(
	ctx context.Context,
	db *badger.DB,
	relations []RelationPattern,
	boundVars map[string]uint64,
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	engine := NewLFTJEngine(db)
	return engine.Execute(ctx, relations, boundVars, resultVars)
}

// RunQueryWithSeeds is a convenience method for running a seeded query.
func RunQueryWithSeeds(
	ctx context.Context,
	db *badger.DB,
	relations []RelationPattern,
	seedVar string,
	seeds iter.Seq2[uint64, error],
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	engine := NewLFTJEngine(db)
	return engine.ExecuteWithSeeds(ctx, relations, seedVar, seeds, resultVars)
}

// Option configures ExecuteOrdered behavior.
type Option func(*orderedConfig)

type orderedConfig struct {
	maxRows int
}

// WithBufferAndSort returns an Option that buffers up to maxRows results
// and yields them sorted by their Canonical() representation.
func WithBufferAndSort(maxRows int) Option {
	return func(cfg *orderedConfig) {
		cfg.maxRows = maxRows
	}
}

// executeConfig holds execution options for Execute and ExecuteOrdered.
type executeConfig struct {
	enableCBO bool
}

// ExecuteOption configures Execute/ExecuteOrdered behavior.
type ExecuteOption func(*executeConfig)

// WithCBO enables cost-based optimization (join order optimization
// based on cardinality estimates). Enabled by default.
func WithCBO(enabled bool) ExecuteOption {
	return func(cfg *executeConfig) {
		cfg.enableCBO = enabled
	}
}

// OptimizeRelations reorders relations to minimize join effort using
// cardinality estimates. This implements greedy CBO (Cost-Based Optimization).
// Returns the reordered relations and the number of relations that had estimates.
func (e *LFTJEngine) OptimizeRelations(ctx context.Context, relations []RelationPattern, boundVars map[string]uint64) ([]RelationPattern, int) {
	n := len(relations)
	if n <= 1 {
		return relations, n
	}

	txn := e.db.NewTransaction(false)
	defer txn.Discard()

	// Estimate cardinality for each relation
	type relCost struct {
		index int
		cost  uint64
	}
	costs := make([]relCost, n)

	for i, rel := range relations {
		card := estimateRelationCardinality(txn, rel)
		costs[i] = relCost{index: i, cost: card}
	}

	// Greedy: pick smallest first
	selected := make([]bool, n)
	optimized := make([]RelationPattern, 0, n)

	estimatedCount := 0
	for len(optimized) < n {
		// Find the unselected relation with the lowest cost
		bestIdx := -1
		var bestCost uint64 = math.MaxUint64
		for i, sel := range selected {
			if sel {
				continue
			}
			if costs[i].cost > 0 && costs[i].cost < bestCost {
				bestCost = costs[i].cost
				bestIdx = i
			} else if bestIdx == -1 && costs[i].cost == 0 {
				bestIdx = i
			}
		}

		if bestIdx >= 0 {
			selected[bestIdx] = true
			optimized = append(optimized, relations[bestIdx])
			if costs[bestIdx].cost > 0 {
				estimatedCount++
			}
		} else {
			break
		}
	}

	// Append any unselected relations (shouldn't happen, but be safe)
	for i := range relations {
		if !selected[i] {
			optimized = append(optimized, relations[i])
		}
	}

	slog.Debug("CBO: optimized join order",
		"original", n,
		"estimated", estimatedCount,
	)

	return optimized, estimatedCount
}

// estimateRelationCardinality estimates the number of distinct values
// for unbound positions in a relation pattern.
func estimateRelationCardinality(txn *badger.Txn, rel RelationPattern) uint64 {
	// Build prefix from bound positions
	prefixKey := make([]byte, keys.TripleKeySize)
	prefixKey[0] = rel.Prefix

	for pos, val := range rel.BoundPositions {
		switch pos {
		case 0:
			binary.BigEndian.PutUint64(prefixKey[1:9], val)
		case 1:
			binary.BigEndian.PutUint64(prefixKey[9:17], val)
		case 2:
			binary.BigEndian.PutUint64(prefixKey[17:25], val)
		}
	}

	// Find which positions are unbound
	unboundPositions := make([]int, 0)
	for pos := 0; pos < 3; pos++ {
		if _, bound := rel.BoundPositions[pos]; !bound {
			unboundPositions = append(unboundPositions, pos)
		}
	}

	if len(unboundPositions) == 0 {
		// All positions bound — exact count via Badger's KeyCount isn't available,
		// so scan a sample
		return estimateKeyCount(txn, prefixKey)
	}

	if len(unboundPositions) > 1 {
		// Multiple unbound positions — estimate tuple count
		return estimateKeyCount(txn, prefixKey)
	}

	// Single unbound position — count distinct values at that position
	varPos := unboundPositions[0]
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   10,
	})
	defer it.Close()

	seen := make(map[uint64]struct{})
	var count uint64
	for it.Seek(prefixKey); it.ValidForPrefix(prefixKey); it.Next() {
		key := it.Item().KeyCopy(nil)
		if len(key) < keys.TripleKeySize {
			continue
		}

		var val uint64
		switch varPos {
		case 0:
			val = binary.BigEndian.Uint64(key[1:9])
		case 1:
			val = binary.BigEndian.Uint64(key[9:17])
		case 2:
			val = binary.BigEndian.Uint64(key[17:25])
		}

		if _, ok := seen[val]; !ok {
			seen[val] = struct{}{}
			count++
		}

		if count >= maxSampleKeys {
			break
		}
	}

	return count
}

// estimateKeyCount estimates the total number of keys matching a prefix.
func estimateKeyCount(txn *badger.Txn, prefix []byte) uint64 {
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   10,
	})
	defer it.Close()

	var count uint64
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		if count >= maxSampleKeys {
			break
		}
	}

	return count
}

// ExecuteOrdered runs a multi-way join and returns deterministically ordered
// results. The joinVars are built from sorted resultVars, ensuring a stable
// join order. Results can optionally be buffered and sorted via WithBufferAndSort.
func (e *LFTJEngine) ExecuteOrdered(
	ctx context.Context,
	relations []RelationPattern,
	boundVars map[string]uint64,
	resultVars []string,
	opts ...Option,
) iter.Seq2[LFTJResult, error] {
	cfg := orderedConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	sortedVars := make([]string, len(resultVars))
	copy(sortedVars, resultVars)
	sort.Strings(sortedVars)

	if cfg.maxRows > 0 {
		return e.executeOrderedBuffered(ctx, relations, boundVars, sortedVars, cfg.maxRows)
	}

	return func(yield func(LFTJResult, error) bool) {
		for raw, err := range e.Execute(ctx, relations, boundVars, sortedVars) {
			if err != nil {
				yield(LFTJResult{}, err)
				return
			}
			r := LFTJResult{
				Vars:   sortedVars,
				Values: make([]uint64, len(sortedVars)),
			}
			for i, v := range sortedVars {
				r.Values[i] = raw[v]
			}
			if !yield(r, nil) {
				return
			}
		}
	}
}

func (e *LFTJEngine) executeOrderedBuffered(
	ctx context.Context,
	relations []RelationPattern,
	boundVars map[string]uint64,
	sortedVars []string,
	maxRows int,
) iter.Seq2[LFTJResult, error] {
	return func(yield func(LFTJResult, error) bool) {
		var results []LFTJResult
		for raw, err := range e.Execute(ctx, relations, boundVars, sortedVars) {
			if err != nil {
				yield(LFTJResult{}, err)
				return
			}
			r := LFTJResult{
				Vars:   sortedVars,
				Values: make([]uint64, len(sortedVars)),
			}
			for i, v := range sortedVars {
				r.Values[i] = raw[v]
			}
			results = append(results, r)
		}

		sort.Slice(results, func(i, j int) bool {
			return results[i].Canonical() < results[j].Canonical()
		})

		if maxRows > 0 && len(results) > maxRows {
			results = results[:maxRows]
		}

		for _, r := range results {
			if !yield(r, nil) {
				return
			}
		}
	}
}
