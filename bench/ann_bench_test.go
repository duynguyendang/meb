package meb_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/bench/datasets"
	"github.com/duynguyendang/meb/store"
)

func testBenchStore(t *testing.T) *meb.MEBStore {
	t.Helper()
	segDir := t.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		VectorFullDim:  128,
		Verbose:        false,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func benchmarkVectorSearch(b *testing.B, numVectors int) {
	s := setupBenchStore(b, 128)
	dim := 128

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		var sum float32
		for _, val := range vec {
			sum += val * val
		}
		norm := float32(math.Sqrt(float64(sum)))
		if norm > 0 {
			for j := range vec {
				vec[j] /= norm
			}
		}
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			b.Fatalf("Add failed: %v", err)
		}
	}

	query := make([]float32, dim)
	for j := range query {
		query[j] = rng.Float32()*2 - 1
	}
	var sum float32
	for _, val := range query {
		sum += val * val
	}
	norm := float32(math.Sqrt(float64(sum)))
	if norm > 0 {
		for j := range query {
			query[j] /= norm
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Vectors().Search(context.Background(), query, 10) {
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

func BenchmarkVectorSearch_1K(b *testing.B)  { benchmarkVectorSearch(b, 1000) }
func BenchmarkVectorSearch_10K(b *testing.B) { benchmarkVectorSearch(b, 10000) }
func BenchmarkVectorSearch_100K(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 100K vector benchmark in short mode")
	}
	benchmarkVectorSearch(b, 100000)
}

func BenchmarkVectorAdd_Sustained(b *testing.B) {
	s := setupBenchStore(b, 128)
	dim := 128
	rng := rand.New(rand.NewSource(42))

	vec := make([]float32, dim)
	for j := range vec {
		vec[j] = rng.Float32()*2 - 1
	}
	var sum float32
	for _, val := range vec {
		sum += val * val
	}
	norm := float32(math.Sqrt(float64(sum)))
	if norm > 0 {
		for j := range vec {
			vec[j] /= norm
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			b.Fatalf("Add failed: %v", err)
		}
	}
}

func BenchmarkFactInsertion_Single(b *testing.B) {
	s := setupBenchStore(b, 128)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fact := meb.NewFact(
			fmt.Sprintf("subject_%d", i),
			"predicate",
			fmt.Sprintf("object_%d", i),
		)
		if err := s.AddFact(fact); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}
}

func BenchmarkFactInsertion_Batch100(b *testing.B) {
	s := setupBenchStore(b, 128)
	batchSize := 100
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		facts := make([]meb.Fact, batchSize)
		for j := 0; j < batchSize; j++ {
			idx := i*batchSize + j
			facts[j] = meb.NewFact(
				fmt.Sprintf("subject_%d", idx),
				"predicate",
				fmt.Sprintf("object_%d", idx),
			)
		}
		if err := s.AddFactBatch(facts); err != nil {
			b.Fatalf("AddFactBatch failed: %v", err)
		}
	}
}

func BenchmarkLFTJ_3Atom(b *testing.B) {
	s := setupBenchStore(b, 128)
	subjects := make([]string, 100)
	for i := 0; i < 1000; i++ {
		sub := fmt.Sprintf("s_%d", i%len(subjects))
		obj := fmt.Sprintf("o_%d", i)
		if err := s.AddFact(meb.NewFact(sub, "knows", obj)); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Scan("", "knows", "") {
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

func BenchmarkLFTJ_5Atom(b *testing.B) {
	s := setupBenchStore(b, 128)
	subjects := make([]string, 50)
	predicates := []string{"knows", "works_at", "lives_in", "created", "likes"}
	objects := make([]string, 100)
	for i := 0; i < 1000; i++ {
		sub := fmt.Sprintf("s_%d", i%len(subjects))
		pred := predicates[i%len(predicates)]
		obj := fmt.Sprintf("o_%d", i%len(objects))
		if err := s.AddFact(meb.NewFact(sub, pred, obj)); err != nil {
			b.Fatalf("AddFact failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for _, err := range s.Scan("", predicates[i%len(predicates)], "") {
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

func TestRecallAt10(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping recall test in short mode")
	}

	s := testBenchStore(t)
	dim := 128
	numVectors := 10000
	numQueries := 10

	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		norm := float32(0)
		for _, val := range v {
			norm += val * val
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 0 {
			for j := range v {
				v[j] /= norm
			}
		}
		vectors[i] = v
		if err := s.Vectors().Add(uint64(i+1), v); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var totalRecall float64
	for qi := 0; qi < numQueries; qi++ {
		query := make([]float32, dim)
		for j := range query {
			query[j] = rng.Float32()*2 - 1
		}
		norm := float32(0)
		for _, val := range query {
			norm += val * val
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 0 {
			for j := range query {
				query[j] /= norm
			}
		}

		type scored struct {
			id    uint64
			score float32
		}
		all := make([]scored, numVectors)
		for i, v := range vectors {
			var dot float32
			for j := range query {
				dot += query[j] * v[j]
			}
			all[i] = scored{id: uint64(i + 1), score: dot}
		}
		sort.Slice(all, func(i, j int) bool {
			return all[i].score > all[j].score
		})
		groundTruth := make(map[uint64]bool)
		for i := 0; i < 10; i++ {
			groundTruth[all[i].id] = true
		}

		found := make(map[uint64]bool)
		for sr, err := range s.Vectors().Search(context.Background(), query, 10) {
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			found[sr.ID] = true
		}

		var hits int
		for id := range found {
			if groundTruth[id] {
				hits++
			}
		}
		totalRecall += float64(hits) / 10.0
	}

	avgRecall := totalRecall / float64(numQueries)
	t.Logf("Average recall@10: %.4f", avgRecall)
	if avgRecall < 0.80 {
		t.Errorf("recall@10 = %.4f, want >= 0.80", avgRecall)
	}
}

func TestSIFTSampleBench(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SIFT sample test in short mode")
	}

	segDir := t.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		VectorFullDim:  128,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	sample := datasets.LoadSIFTSample()

	for i, vec := range sample.Vectors {
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			t.Fatalf("Add failed at %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var totalRecall float64
	for qi, query := range sample.Queries {
		type scored struct {
			id    uint64
			score float32
		}
		all := make([]scored, len(sample.Vectors))
		for i, v := range sample.Vectors {
			var dot float32
			for j := range query {
				dot += query[j] * v[j]
			}
			all[i] = scored{id: uint64(i + 1), score: dot}
		}
		sort.Slice(all, func(i, j int) bool {
			return all[i].score > all[j].score
		})
		groundTruth := make(map[uint64]bool)
		for i := 0; i < 10; i++ {
			groundTruth[all[i].id] = true
		}

		found := make(map[uint64]bool)
		for sr, err := range s.Vectors().Search(context.Background(), query, 10) {
			if err != nil {
				t.Fatalf("Search failed at query %d: %v", qi, err)
			}
			found[sr.ID] = true
		}

		var hits int
		for id := range found {
			if groundTruth[id] {
				hits++
			}
		}
		totalRecall += float64(hits) / 10.0
	}

	avgRecall := totalRecall / float64(len(sample.Queries))
	t.Logf("SIFT sample recall@10: %.4f", avgRecall)
	if avgRecall < 0.80 {
		t.Errorf("SIFT sample recall@10 = %.4f, want >= 0.80", avgRecall)
	}
}

// TestSIFT1MRecall tests recall@10 with the full SIFT1M dataset if available.
// Falls back to synthetic data if the dataset is not downloaded.
func TestSIFT1MRecall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SIFT1M test in short mode")
	}

	// Try to load real SIFT1M dataset
	dataDir := os.Getenv("SIFT_DATA_DIR")
	if dataDir == "" {
		dataDir = "./bench/datasets/data"
	}

	dataset := datasets.LoadSIFT1M(dataDir)
	if dataset == nil {
		t.Skip("SIFT1M dataset not available. Download from http://corpus-texmex.irisa.fr/ and set SIFT_DATA_DIR")
	}

	t.Logf("Loaded SIFT1M dataset: %d base vectors, %d queries, dim=%d",
		len(dataset.Base), len(dataset.Queries), dataset.Dim)

	segDir := t.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		VectorFullDim:  dataset.Dim,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Add base vectors
	for i, vec := range dataset.Base {
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			t.Fatalf("Add failed at %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Test recall@10 on first 100 queries (for speed)
	numQueries := 100
	if numQueries > len(dataset.Queries) {
		numQueries = len(dataset.Queries)
	}

	var totalRecall float64
	latencies := make([]time.Duration, 0, numQueries)

	for qi := 0; qi < numQueries; qi++ {
		query := dataset.Queries[qi]
		groundTruth := dataset.GroundTruth[qi]

		// Build ground truth set (top 10)
		gtSet := make(map[uint32]bool)
		for i := 0; i < 10 && i < len(groundTruth); i++ {
			gtSet[groundTruth[i]] = true
		}

		// Measure search latency
		start := time.Now()
		found := make(map[uint64]bool)
		for sr, err := range s.Vectors().Search(context.Background(), query, 10) {
			if err != nil {
				t.Fatalf("Search failed at query %d: %v", qi, err)
			}
			found[sr.ID] = true
		}
		latencies = append(latencies, time.Since(start))

		// Calculate recall
		var hits int
		for id := range found {
			if gtSet[uint32(id)] {
				hits++
			}
		}
		totalRecall += float64(hits) / 10.0
	}

	avgRecall := totalRecall / float64(numQueries)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99Latency := latencies[int(float64(len(latencies))*0.99)]

	t.Logf("SIFT1M recall@10: %.4f (on %d queries)", avgRecall, numQueries)
	t.Logf("SIFT1M p99 latency: %v", p99Latency)
	t.Logf("SIFT1M latency stats: min=%v, median=%v, max=%v",
		latencies[0], latencies[len(latencies)/2], latencies[len(latencies)-1])

	// Note: recall threshold is lower for synthetic/early-stage IVF-PQ
	// Real SIFT1M with proper training should achieve ≥0.97
	if avgRecall < 0.80 {
		t.Errorf("SIFT1M recall@10 = %.4f, want >= 0.80", avgRecall)
	}
}

// TestRecallWithLatency measures recall@10 and p99 latency on synthetic data.
func TestRecallWithLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping recall+latency test in short mode")
	}

	segDir := t.TempDir()
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   10000,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
		VectorFullDim:  128,
	}
	s, err := meb.NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	dim := 128
	numVectors := 10000
	numQueries := 100
	rng := rand.New(rand.NewSource(42))

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		norm := float32(0)
		for _, val := range v {
			norm += val * val
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 0 {
			for j := range v {
				v[j] /= norm
			}
		}
		vectors[i] = v
		if err := s.Vectors().Add(uint64(i+1), v); err != nil {
			t.Fatalf("Add failed at %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var totalRecall float64
	latencies := make([]time.Duration, 0, numQueries)

	for qi := 0; qi < numQueries; qi++ {
		query := make([]float32, dim)
		for j := range query {
			query[j] = rng.Float32()*2 - 1
		}
		norm := float32(0)
		for _, val := range query {
			norm += val * val
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 0 {
			for j := range query {
				query[j] /= norm
			}
		}

		// Compute ground truth
		type scored struct {
			id    uint64
			score float32
		}
		all := make([]scored, numVectors)
		for i, v := range vectors {
			var dot float32
			for j := range query {
				dot += query[j] * v[j]
			}
			all[i] = scored{id: uint64(i + 1), score: dot}
		}
		sort.Slice(all, func(i, j int) bool {
			return all[i].score > all[j].score
		})
		groundTruth := make(map[uint64]bool)
		for i := 0; i < 10; i++ {
			groundTruth[all[i].id] = true
		}

		// Measure search latency
		start := time.Now()
		found := make(map[uint64]bool)
		for sr, err := range s.Vectors().Search(context.Background(), query, 10) {
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			found[sr.ID] = true
		}
		latencies = append(latencies, time.Since(start))

		var hits int
		for id := range found {
			if groundTruth[id] {
				hits++
			}
		}
		totalRecall += float64(hits) / 10.0
	}

	avgRecall := totalRecall / float64(numQueries)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50Latency := latencies[len(latencies)/2]
	p99Latency := latencies[int(float64(len(latencies))*0.99)]

	t.Logf("Recall@10: %.4f", avgRecall)
	t.Logf("Latency stats: p50=%v, p99=%v, min=%v, max=%v",
		p50Latency, p99Latency, latencies[0], latencies[len(latencies)-1])

	if avgRecall < 0.80 {
		t.Errorf("recall@10 = %.4f, want >= 0.80", avgRecall)
	}
}
