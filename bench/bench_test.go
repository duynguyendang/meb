package meb_test

import (
	"log/slog"
	"math/rand"
	"os"
	"testing"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}

func setupBenchStore(b *testing.B) *meb.MEBStore {
	b.Helper()
	segDir := b.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		Verbose:        false,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		b.Fatalf("NewMEBStore: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := 0; i < dim; i++ {
		v[i] = rand.Float32()*2 - 1.0
	}
	return v
}

func BenchmarkVectorAdd(b *testing.B) {
	s := setupBenchStore(b)
	vec := randomVector(1536)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(i + 1)
		if err := s.Vectors().Add(id, vec); err != nil {
			b.Fatalf("Add failed: %v", err)
		}
	}
}

func BenchmarkVectorSearch(b *testing.B) {
	s := setupBenchStore(b)
	numVectors := 10000
	vectors := make([][]float32, numVectors)

	for i := 0; i < numVectors; i++ {
		vec := randomVector(1536)
		vectors[i] = vec
		s.Vectors().Add(uint64(i+1), vec)
	}

	query := vectors[0]
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Vectors().Search(query, 10) {
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
			count++
		}
		if count == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkFactInsertion(b *testing.B) {
	s := setupBenchStore(b)
	subjects := make([]string, 1000)
	predicates := []string{"knows", "works_at", "lives_in", "created"}
	objects := make([]string, 500)

	for i := range subjects {
		subjects[i] = "subject_" + string(rune('A'+i%26))
	}
	for i := range objects {
		objects[i] = "object_" + string(rune('A'+i%26))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subject := subjects[i%len(subjects)]
		predicate := predicates[i%len(predicates)]
		object := objects[i%len(objects)]

		if err := s.AddFact(meb.NewFact(subject, predicate, object)); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}
}

func BenchmarkFactInsertionBatch(b *testing.B) {
	s := setupBenchStore(b)
	batchSize := 100

	subjects := make([]string, 1000)
	predicates := []string{"knows", "works_at", "lives_in", "created"}
	objects := make([]string, 500)

	for i := range subjects {
		subjects[i] = "subject_" + string(rune('A'+i%26))
	}
	for i := range objects {
		objects[i] = "object_" + string(rune('A'+i%26))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		facts := make([]meb.Fact, batchSize)
		for j := 0; j < batchSize; j++ {
			idx := i*batchSize + j
			facts[j] = meb.NewFact(
				subjects[idx%len(subjects)],
				predicates[idx%len(predicates)],
				objects[idx%len(objects)],
			)
		}

		if err := s.AddFactBatch(facts); err != nil {
			b.Fatalf("AddFactBatch failed: %v", err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	s := setupBenchStore(b)
	numFacts := 10000

	for i := 0; i < numFacts; i++ {
		subject := "subject_" + string(rune('A'+i%26))
		predicate := "predicate_" + string(rune('A'+i%10))
		object := "object_" + string(rune('A'+i%50))

		if err := s.AddFact(meb.NewFact(subject, predicate, object)); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Scan("subject_A", "", "") {
			if err != nil {
				b.Fatalf("Scan failed: %v", err)
			}
			count++
		}
		if count == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkScanKeyOnly(b *testing.B) {
	s := setupBenchStore(b)
	numFacts := 100000

	for i := 0; i < numFacts; i++ {
		subject := "subject_" + string(rune('A'+i%26))
		predicate := "predicate_" + string(rune('A'+i%10))
		object := "object_" + string(rune('A'+i%50))

		if err := s.AddFact(meb.NewFact(subject, predicate, object)); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Scan("subject_A", "", "") {
			if err != nil {
				b.Fatalf("Scan failed: %v", err)
			}
			count++
		}
		if count == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkDocumentAdd(b *testing.B) {
	s := setupBenchStore(b)
	content := []byte("This is a test document with some content for benchmarking purposes.")
	vec := randomVector(1536)
	metadata := map[string]any{
		"author": "benchmark",
		"type":   "test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docKey := "doc_" + string(rune('A'+i%26))
		if err := s.AddDocument(docKey, content, vec, metadata); err != nil {
			b.Fatalf("AddDocument failed: %v", err)
		}
	}
}

func BenchmarkHybridSearch(b *testing.B) {
	s := setupBenchStore(b)
	numDocs := 1000

	for i := 0; i < numDocs; i++ {
		docKey := "doc_" + string(rune('A'+i%26))
		content := []byte("Document content for benchmarking")
		vec := randomVector(1536)
		metadata := map[string]any{
			"author": "benchmark",
			"type":   "test",
		}

		if err := s.AddDocument(docKey, content, vec, metadata); err != nil {
			b.Fatalf("AddDocument failed: %v", err)
		}
	}

	query := randomVector(1536)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		results, err := s.Find().
			SimilarTo(query).
			Limit(10).
			Execute()
		if err != nil {
			b.Fatalf("Find.Execute failed: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkDictionaryGetOrCreate(b *testing.B) {
	s := setupBenchStore(b)
	words := make([]string, 10000)
	for i := range words {
		words[i] = "word_" + string(rune('A'+i%26))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		word := words[i%len(words)]
		s.LookupID(word)
	}
}

func BenchmarkDeleteFactsBySubject(b *testing.B) {
	s := setupBenchStore(b)
	numSubjects := 100
	factsPerSubject := 100

	for i := 0; i < numSubjects; i++ {
		subject := "subject_" + string(rune('A'+i%26))
		for j := 0; j < factsPerSubject; j++ {
			predicate := "predicate_" + string(rune('A'+j%10))
			object := "object_" + string(rune('A'+j%50))
			if err := s.AddFact(meb.NewFact(subject, predicate, object)); err != nil {
				b.Fatalf("AddFact failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subject := "subject_" + string(rune('A'+i%26))
		if err := s.DeleteFactsBySubject(subject); err != nil {
			b.Fatalf("DeleteFactsBySubject failed: %v", err)
		}
	}
}
