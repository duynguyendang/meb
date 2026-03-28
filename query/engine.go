package query

import (
	"context"
	"iter"

	"github.com/dgraph-io/badger/v4"
)

// QueryResult represents a single join result with bound variables.
type QueryResult struct {
	Bindings map[string]uint64
}

// ExecuteWithSeeds runs an LFTJ join seeded with candidate IDs from vector search.
// This is the Neuro-First pipeline: TurboQuant seeds → LFTJ join → results.
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

			for result, err := range e.Execute(relations, boundVars, resultVars) {
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
	db *badger.DB,
	relations []RelationPattern,
	boundVars map[string]uint64,
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	engine := NewLFTJEngine(db)
	return engine.Execute(relations, boundVars, resultVars)
}

// RunQueryWithSeeds is a convenience method for running a seeded query.
func RunQueryWithSeeds(
	db *badger.DB,
	relations []RelationPattern,
	seedVar string,
	seeds iter.Seq2[uint64, error],
	resultVars []string,
) iter.Seq2[map[string]uint64, error] {
	engine := NewLFTJEngine(db)
	return engine.ExecuteWithSeeds(context.Background(), relations, seedVar, seeds, resultVars)
}
