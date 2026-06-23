package meb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/query"
	"github.com/duynguyendang/meb/store"
)

// BenchmarkLFTJ_3Atom_CBO measures the performance improvement from CBO
// for a 3-atom join where relation order matters.
// Pattern: (?a, knows, ?b) ∧ (?b, works_at, "Acme") ∧ (?a, lives_in, "NYC")
func BenchmarkLFTJ_3Atom_CBO(b *testing.B) {
	s := setupBenchStore(b)

	// Insert facts with skewed cardinality:
	// - "knows" has few distinct objects (100)
	// - "works_at" has many distinct subjects (5000)
	// - "lives_in" has many distinct subjects (8000)
	// CBO should order: knows (smallest) → works_at → lives_in
	for i := 0; i < 500; i++ {
		subject := fmt.Sprintf("person_%d", i)
		// Each person knows ~5 other people
		for j := 0; j < 5; j++ {
			obj := fmt.Sprintf("person_%d", (i+j+1)%500)
			s.AddFact(meb.NewFact(subject, "knows", obj))
		}
		// Each person works at one of 5 companies
		company := fmt.Sprintf("company_%d", i%5)
		s.AddFact(meb.NewFact(subject, "works_at", company))
		// Each person lives in one of 3 cities
		city := fmt.Sprintf("city_%d", i%3)
		s.AddFact(meb.NewFact(subject, "lives_in", city))
	}

	// Resolve IDs for bound values
	acmeID, _ := s.Dict().GetID("company_0")
	nycID, _ := s.Dict().GetID("city_0")

	// CBO-enabled query
	b.Run("CBO-on", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count := 0
			for range s.LFTJEngine().ExecuteOrdered(
				context.Background(),
				[]query.RelationPattern{
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "a", 2: "b"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "knows")},
					},
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "b"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "works_at"), 2: acmeID},
					},
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "a"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "lives_in"), 2: nycID},
					},
				},
				nil,
				[]string{"a", "b"},
			) {
				count++
				_ = count
			}
		}
	})

	// CBO-disabled query (same relations, but CBO is off via WithCBO(false))
	b.Run("CBO-off", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count := 0
			for range s.LFTJEngine().Execute(
				context.Background(),
				[]query.RelationPattern{
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "a", 2: "b"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "knows")},
					},
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "b"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "works_at"), 2: acmeID},
					},
					{
						Prefix:            keys.TripleSPOPrefix,
						VariablePositions: map[int]string{0: "a"},
						BoundPositions:    map[int]uint64{1: mustGetID(s, "lives_in"), 2: nycID},
					},
				},
				nil,
				[]string{"a", "b"},
				query.WithCBO(false),
			) {
				count++
				_ = count
			}
		}
	})
}

// BenchmarkHybridSearchWithFilters measures the performance improvement
// from predicate pushdown when filters are selective.
func BenchmarkHybridSearchWithFilters(b *testing.B) {
	dim := 128
	segDir := b.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		VectorFullDim:  dim,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		Verbose:        false,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		b.Fatalf("NewMEBStore: %v", err)
	}
	b.Cleanup(func() { s.Close() })

	numDocs := 5000
	// Only 5% have category "rare"
	for i := 0; i < numDocs; i++ {
		docKey := fmt.Sprintf("doc_%d", i)
		vec := randomVector(dim)
		category := "common"
		if i%20 == 0 {
			category = "rare"
		}
		metadata := map[string]any{
			"category": category,
		}
		content := []byte(fmt.Sprintf("Document %d content", i))
		if err := s.AddDocument(docKey, content, vec, metadata); err != nil {
			b.Fatalf("AddDocument failed: %v", err)
		}
	}

	query := randomVector(dim)

	// Without predicate pushdown (vector-first: run search then filter)
	b.Run("vector-first", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := s.Find().
				SimilarTo(query).
				Where("category", "rare").
				Limit(10).
				Execute(context.Background())
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}
			_ = results
		}
	})

	// With predicate pushdown (filter-first: evaluate filters then search)
	b.Run("filter-first", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			results, err := s.Find().
				SimilarTo(query).
				Where("category", "rare").
				FilterFirst().
				Limit(10).
				Execute(context.Background())
			if err != nil {
				b.Fatalf("Execute failed: %v", err)
			}
			_ = results
		}
	})
}

// BenchmarkPlanCache measures the performance improvement from caching
// optimized query plans for repeated queries.
func BenchmarkPlanCache(b *testing.B) {
	s := setupBenchStore(b)

	// Insert facts
	for i := 0; i < 500; i++ {
		subject := fmt.Sprintf("entity_%d", i)
		s.AddFact(meb.NewFact(subject, "type", "person"))
		s.AddFact(meb.NewFact(subject, "tag", fmt.Sprintf("tag_%d", i%20)))
		s.AddFact(meb.NewFact(subject, "group", fmt.Sprintf("group_%d", i%10)))
	}

	typeID, _ := s.Dict().GetID("type")
	personID, _ := s.Dict().GetID("person")

	// Run the same query multiple times to measure cache benefit
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		for range s.LFTJEngine().ExecuteOrdered(
			context.Background(),
			[]query.RelationPattern{
				{
					Prefix:            keys.TripleSPOPrefix,
					VariablePositions: map[int]string{0: "x"},
					BoundPositions:    map[int]uint64{1: typeID, 2: personID},
				},
				{
					Prefix:            keys.TripleSPOPrefix,
					VariablePositions: map[int]string{2: "tag"},
					BoundPositions:    map[int]uint64{0: typeID, 1: personID},
				},
			},
			nil,
			[]string{"x", "tag"},
		) {
			count++
			_ = count
		}
	}
}

func mustGetID(s *meb.MEBStore, key string) uint64 {
	id, err := s.Dict().GetID(key)
	if err != nil {
		panic(fmt.Sprintf("GetID(%q): %v", key, err))
	}
	return id
}
