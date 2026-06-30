package meb

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"github.com/duynguyendang/meb/dict"
	"github.com/duynguyendang/meb/query"
	"github.com/duynguyendang/meb/utils"
	"github.com/duynguyendang/meb/vector"
)

const (
	DefaultCandidateMultiplier = 10
)

type QueryFilter struct {
	Predicate string
	Object    interface{}
}

type Store interface {
	Vectors() *vector.VectorRegistry
	Scan(s, p, o string) iter.Seq2[Fact, error]
	GetContent(id uint64) ([]byte, error)
	ResolveID(id uint64) (string, error)
	LFTJEngine() *query.LFTJEngine
	Dict() dict.Dictionary
	Exists(s, p, o string) bool
}

type Builder struct {
	store               Store
	vectorQuery         []float32
	threshold           float32
	filters             []QueryFilter
	limit               int
	candidateMultiplier int
	topicID             uint32
	relations           []query.RelationPattern
	resultVars          []string
	filterFirst         bool // predicate pushdown: evaluate filters before vector search
}

func NewBuilder(store Store) *Builder {
	return &Builder{
		store:               store,
		limit:               10,
		candidateMultiplier: DefaultCandidateMultiplier,
	}
}

func (b *Builder) SimilarTo(vec []float32) *Builder {
	b.vectorQuery = vec
	return b
}

func (b *Builder) SimilarToWithThreshold(vec []float32, threshold float32) *Builder {
	b.vectorQuery = vec
	b.threshold = threshold
	return b
}

func (b *Builder) Where(predicate string, object interface{}) *Builder {
	b.filters = append(b.filters, QueryFilter{
		Predicate: predicate,
		Object:    object,
	})
	return b
}

func (b *Builder) Limit(n int) *Builder {
	b.limit = n
	return b
}

func (b *Builder) CandidateMultiplier(multiplier int) *Builder {
	b.candidateMultiplier = multiplier
	return b
}

// FilterFirst enables candidate-set pre-filtering for this query.
// When enabled, filters are evaluated against the graph index FIRST,
// building a set of matching subject IDs. Vector search results that
// aren't in this set are then skipped in the result loop (post-filter).
//
// Important: This is NOT true predicate pushdown — vector search still
// scans all vectors and computes similarities for all candidates. The
// benefit comes from skipping dictionary lookups and content fetches
// for non-matching vectors. Most effective when:
//   - Filters are highly selective (< 10% match rate)
//   - Content fetch or dict lookup is expensive relative to vector search
func (b *Builder) FilterFirst() *Builder {
	b.filterFirst = true
	return b
}

// InTopic restricts the search to a specific topic.
// The TopicID is used for topic-aware vector search and scan operations.
func (b *Builder) InTopic(topicID uint32) *Builder {
	b.topicID = topicID
	return b
}

// JoinWithLFTJ adds LFTJ relations for structural expansion after vector search.
// The seedVar must match a variable name used in the relations.
// Results are streamed — no intermediate materialization.
func (b *Builder) JoinWithLFTJ(relations []query.RelationPattern, resultVars []string) *Builder {
	b.relations = relations
	b.resultVars = resultVars
	return b
}

// Execute runs the query using RBO to automatically select the optimal strategy.
// FilterFirst() is kept as an explicit override for manual control.
func (b *Builder) Execute(ctx context.Context) ([]Result, error) {
	if len(b.vectorQuery) == 0 {
		return nil, fmt.Errorf("query must include SimilarTo() vector search")
	}

	plan := query.Optimize(
		b.hasVectorQuery(),
		b.filterCount(),
		b.estimatedSelectivity(),
	)

	if b.filterFirst {
		plan.Type = query.GraphFirst
	}

	return b.executePlan(ctx, plan)
}

func (b *Builder) hasVectorQuery() bool {
	return len(b.vectorQuery) > 0
}

func (b *Builder) filterCount() int {
	return len(b.filters)
}

func (b *Builder) estimatedSelectivity() float64 {
	if len(b.filters) == 0 {
		return 1.0
	}
	return 0.1
}

func (b *Builder) executePlan(ctx context.Context, plan query.ExecutionPlan) ([]Result, error) {
	candidateK := b.limit * b.candidateMultiplier
	if candidateK < 100 {
		candidateK = 100
	}

	var filterCandidateSet map[uint64]struct{}
	usePushdown := b.filterFirst && len(b.filters) > 0

	if usePushdown {
		var err error
		filterCandidateSet, err = b.buildFilterCandidateSet(ctx)
		if err != nil {
			return nil, fmt.Errorf("filter candidate set: %w", err)
		}
		slog.Debug("predicate pushdown",
			"filters", len(b.filters),
			"candidates", len(filterCandidateSet),
			"candidateK", candidateK,
		)
	}

	var vecIter iter.Seq2[vector.SearchResult, error]
	if b.topicID > 0 {
		vecIter = b.store.Vectors().SearchInTopic(ctx, b.topicID, b.vectorQuery, candidateK)
	} else {
		vecIter = b.store.Vectors().Search(ctx, b.vectorQuery, candidateK)
	}

	results := make([]Result, 0, b.limit)

	if len(b.relations) > 0 {
		return b.executeWithLFTJ(ctx, vecIter, results)
	}

	for vecResult, err := range vecIter {
		if err != nil {
			return nil, fmt.Errorf("vector search failed: %w", err)
		}

		if b.threshold > 0 && vecResult.Score < b.threshold {
			continue
		}

		if usePushdown {
			if _, ok := filterCandidateSet[vecResult.ID]; !ok {
				continue
			}
		}

		candidateKey, err := b.store.ResolveID(vecResult.ID)
		if err != nil {
			continue
		}

		if b.matchesFilters(candidateKey) {
			contentBytes, err := b.store.GetContent(vecResult.ID)
			contentStr := ""
			if err == nil && contentBytes != nil {
				contentStr = utils.BytesToString(contentBytes)
			}

			results = append(results, Result{
				ID:      vecResult.ID,
				Key:     candidateKey,
				Score:   vecResult.Score,
				Content: contentStr,
			})

			if len(results) >= b.limit {
				break
			}
		}
	}

	return results, nil
}

// Returns a map of candidate IDs matching all filters, or nil if no filter is set.
// Scan errors (I/O failures) are propagated; per-subject GetID misses are silently
// excluded since they indicate a dict inconsistency rather than a query error.
func (b *Builder) buildFilterCandidateSet(ctx context.Context) (map[uint64]struct{}, error) {
	if len(b.filters) == 0 {
		return nil, nil
	}

	filter := b.filters[0]
	candidates := make(map[uint64]struct{})

	scanIter := b.store.Scan("", filter.Predicate, fmt.Sprintf("%v", filter.Object))
	for fact, err := range scanIter {
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		id, err := b.store.Dict().GetID(fact.Subject)
		if err != nil {
			continue
		}
		candidates[id] = struct{}{}
	}

	for _, f := range b.filters[1:] {
		for id := range candidates {
			key, err := b.store.ResolveID(id)
			if err != nil {
				delete(candidates, id)
				continue
			}
			if !b.store.Exists(key, f.Predicate, fmt.Sprintf("%v", f.Object)) {
				delete(candidates, id)
			}
		}
	}

	return candidates, nil
}

func (b *Builder) executeWithLFTJ(ctx context.Context, vecIter iter.Seq2[vector.SearchResult, error], results []Result) ([]Result, error) {
	engine := b.store.LFTJEngine()
	if engine == nil {
		return nil, fmt.Errorf("LFTJ engine not available")
	}

	seedIDs := func(yield func(uint64, error) bool) {
		for vecResult, err := range vecIter {
			if err != nil {
				yield(0, err)
				return
			}
			if b.threshold > 0 && vecResult.Score < b.threshold {
				continue
			}
			if !yield(vecResult.ID, nil) {
				return
			}
		}
	}

	boundVars := map[string]uint64{}
	for _, rel := range b.relations {
		for _, name := range rel.VariablePositions {
			if _, ok := boundVars[name]; !ok {
				boundVars[name] = 0
			}
		}
	}

	seedVar := ""
	for _, rel := range b.relations {
		for _, name := range rel.VariablePositions {
			if boundVars[name] == 0 {
				seedVar = name
				boundVars[name] = 0
				break
			}
		}
		if seedVar != "" {
			break
		}
	}
	if seedVar == "" {
		seedVar = "seed"
	}

	for joinResult, err := range engine.ExecuteWithSeeds(
		ctx,
		b.relations,
		seedVar,
		seedIDs,
		b.resultVars,
	) {
		if err != nil {
			return nil, fmt.Errorf("LFTJ join failed: %w", err)
		}

		var id uint64
		for _, v := range joinResult {
			id = v
			break
		}
		if id == 0 {
			continue
		}

		candidateKey, err := b.store.ResolveID(id)
		if err != nil {
			continue
		}

		contentBytes, err := b.store.GetContent(id)
		contentStr := ""
		if err == nil && contentBytes != nil {
			contentStr = utils.BytesToString(contentBytes)
		}

		results = append(results, Result{
			ID:      id,
			Key:     candidateKey,
			Score:   1.0,
			Content: contentStr,
		})

		if len(results) >= b.limit {
			break
		}
	}

	return results, nil
}

func (b *Builder) matchesFilters(candidateKey string) bool {
	for _, filter := range b.filters {
		if !b.store.Exists(candidateKey, filter.Predicate, fmt.Sprintf("%v", filter.Object)) {
			return false
		}
	}
	return true
}
