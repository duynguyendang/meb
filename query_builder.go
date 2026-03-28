package meb

import (
	"fmt"
	"iter"

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
}

type Builder struct {
	store               Store
	vectorQuery         []float32
	threshold           float32
	filters             []QueryFilter
	limit               int
	candidateMultiplier int
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

func (b *Builder) Execute() ([]Result, error) {
	if len(b.vectorQuery) == 0 {
		return nil, fmt.Errorf("query must include SimilarTo() vector search")
	}

	candidateK := b.limit * b.candidateMultiplier
	if candidateK < 100 {
		candidateK = 100
	}

	vectorResults, err := b.store.Vectors().Search(b.vectorQuery, candidateK)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	results := make([]Result, 0, b.limit)

	for _, vecResult := range vectorResults {
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
