package meb

import (
	"context"
	"fmt"
	"iter"

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

func (b *Builder) Execute() ([]Result, error) {
	if len(b.vectorQuery) == 0 {
		return nil, fmt.Errorf("query must include SimilarTo() vector search")
	}

	candidateK := b.limit * b.candidateMultiplier
	if candidateK < 100 {
		candidateK = 100
	}

	var vecIter iter.Seq2[vector.SearchResult, error]
	if b.topicID > 0 {
		vecIter = b.store.Vectors().SearchInTopic(b.topicID, b.vectorQuery, candidateK)
	} else {
		vecIter = b.store.Vectors().Search(b.vectorQuery, candidateK)
	}

	results := make([]Result, 0, b.limit)

	if len(b.relations) > 0 {
		return b.executeWithLFTJ(vecIter, results)
	}

	for vecResult, err := range vecIter {
		if err != nil {
			return nil, fmt.Errorf("vector search failed: %w", err)
		}

		if b.threshold > 0 && vecResult.Score < b.threshold {
			continue
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

func (b *Builder) executeWithLFTJ(vecIter iter.Seq2[vector.SearchResult, error], results []Result) ([]Result, error) {
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
		for pos, name := range rel.VariablePositions {
			if _, ok := boundVars[name]; !ok {
				boundVars[name] = 0
			}
			_ = pos
		}
	}

	seedVar := ""
	for _, rel := range b.relations {
		for pos, name := range rel.VariablePositions {
			if boundVars[name] == 0 {
				seedVar = name
				boundVars[name] = 0
				_ = pos
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
		context.Background(),
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
		matched := false

		for _, err := range b.store.Scan(candidateKey, filter.Predicate, fmt.Sprintf("%v", filter.Object)) {
			if err != nil {
				return false
			}
			matched = true
			break
		}

		if !matched {
			return false
		}
	}

	return true
}
