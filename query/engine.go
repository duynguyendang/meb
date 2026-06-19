package query

import (
	"context"
	"iter"
	"log/slog"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// QueryResult represents a single join result with bound variables.
type QueryResult struct {
	Bindings map[string]uint64
}

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
		var overflow bool
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
			if len(results) < maxRows {
				results = append(results, r)
			} else {
				overflow = true
			}
		}

		if overflow {
			slog.Warn("LFTJ buffer overflow",
				"bufferSize", maxRows,
				"action", "dropping excess rows",
			)
		}

		sort.Slice(results, func(i, j int) bool {
			return results[i].Canonical() < results[j].Canonical()
		})

		for _, r := range results {
			if !yield(r, nil) {
				return
			}
		}
	}
}
