// Command bench runs the MEB benchmark suite and outputs a JSON report.
//
// Usage:
//
//	go run ./cmd/bench
//
// Outputs a JSON report with recall@10, p50/p95/p99 latency, and throughput
// for each benchmark scenario.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/duynguyendang/meb"
	"github.com/duynguyendang/meb/store"
	"github.com/duynguyendang/meb/vector"
)

type BenchReport struct {
	Timestamp   string              `json:"timestamp"`
	Scenarios   []ScenarioResult    `json:"scenarios"`
}

type ScenarioResult struct {
	Name       string  `json:"name"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	RecallAt10 float64 `json:"recall_at_10,omitempty"`
}

func main() {
	report := BenchReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// Brute-force vector search benchmarks
	for _, n := range []int{1000, 10000} {
		scenario := benchVectorSearch(n)
		report.Scenarios = append(report.Scenarios, scenario)
	}

	// IVF-PQ benchmarks
	for _, n := range []int{10000} {
		scenario := benchIVFPQSearch(n)
		report.Scenarios = append(report.Scenarios, scenario)
	}

	// HNSW benchmarks
	for _, n := range []int{10000} {
		scenario := benchHNSWSearch(n)
		report.Scenarios = append(report.Scenarios, scenario)
	}

	// Vector add throughput
	report.Scenarios = append(report.Scenarios, benchVectorAdd())

	// Recall@10 (brute-force)
	report.Scenarios = append(report.Scenarios, benchRecall())

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		log.Fatalf("failed to encode report: %v", err)
	}
}

func newBenchStore() (*meb.MEBStore, func()) {
	segDir, err := os.MkdirTemp("", "meb-bench-*")
	if err != nil {
		log.Fatalf("TempDir: %v", err)
	}
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
		log.Fatalf("NewMEBStore: %v", err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(segDir)
	}
}

func newBenchStoreWithIVFPQ() (*meb.MEBStore, func()) {
	s, cleanup := newBenchStore()
	cfg := vector.DefaultIVFPQConfig()
	cfg.NumCentroids = 256
	cfg.NumSubSpaces = 32
	cfg.NProbe = 16
	cfg.BatchSize = 1000
	if err := s.EnableIVFPQ(cfg); err != nil {
		log.Fatalf("EnableIVFPQ: %v", err)
	}
	return s, cleanup
}

func newBenchStoreWithHNSW() (*meb.MEBStore, func()) {
	s, cleanup := newBenchStore()
	cfg := vector.DefaultHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	cfg.EfSearch = 32
	if err := s.EnableHNSW(cfg); err != nil {
		log.Fatalf("EnableHNSW: %v", err)
	}
	return s, cleanup
}

func benchVectorSearch(numVectors int) ScenarioResult {
	s, cleanup := newBenchStore()
	defer cleanup()

	dim := 128
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numVectors; i++ {
		vec := randomUnitVector(rng, dim)
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			log.Fatalf("Add failed: %v", err)
		}
	}

	query := randomUnitVector(rng, dim)
	time.Sleep(100 * time.Millisecond)

	const numTrials = 50
	var latencies []float64
	for i := 0; i < numTrials; i++ {
		start := time.Now()
		count := 0
		for _, err := range s.Vectors().Search(context.Background(), query, 10) {
			if err != nil {
				log.Fatalf("Search failed: %v", err)
			}
			count++
		}
		if count == 0 {
			log.Fatal("expected results")
		}
		latencies = append(latencies, float64(time.Since(start).Microseconds())/1000.0)
	}

	sort.Float64s(latencies)

	// Compute throughput (vectors per second)
	totalTime := time.Duration(0)
	for _, l := range latencies {
		totalTime += time.Duration(l * float64(time.Millisecond))
	}
	throughput := float64(numTrials) / totalTime.Seconds()

	return ScenarioResult{
		Name:       fmt.Sprintf("VectorSearch_%dK", numVectors/1000),
		OpsPerSec:  throughput,
		P50Ms:      percentile(latencies, 50),
		P95Ms:      percentile(latencies, 95),
		P99Ms:      percentile(latencies, 99),
	}
}

func benchVectorAdd() ScenarioResult {
	s, cleanup := newBenchStore()
	defer cleanup()

	dim := 128
	rng := rand.New(rand.NewSource(42))
	vec := randomUnitVector(rng, dim)

	const numVectors = 10000
	start := time.Now()
	for i := 0; i < numVectors; i++ {
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			log.Fatalf("Add failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	throughput := float64(numVectors) / elapsed.Seconds()
	return ScenarioResult{
		Name:      "VectorAdd_Sustained",
		OpsPerSec: throughput,
	}
}

func benchIVFPQSearch(numVectors int) ScenarioResult {
	s, cleanup := newBenchStoreWithIVFPQ()
	defer cleanup()

	dim := 128
	rng := rand.New(rand.NewSource(42))

	topicID := uint32(1)

	// Add vectors via IVF-PQ
	vectors := make([][]float32, numVectors)
	if err := s.Update(func(txn *meb.StoreTxn) error {
		for i := 0; i < numVectors; i++ {
			vec := randomUnitVector(rng, dim)
			vectors[i] = vec
			if err := txn.AddIVFVector(topicID, uint64(i+1), vec); err != nil {
				return fmt.Errorf("AddIVFVector failed: %v", err)
			}
		}
		return nil
	}); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	// Train IVF-PQ
	log.Printf("Training IVF-PQ on %d vectors...", numVectors)
	if err := s.TrainIVFPQ(topicID); err != nil {
		log.Fatalf("TrainIVFPQ failed: %v", err)
	}

	query := randomUnitVector(rng, dim)
	time.Sleep(100 * time.Millisecond)

	const numTrials = 50
	var latencies []float64
	for i := 0; i < numTrials; i++ {
		start := time.Now()
		var count int
		if err := s.View(func(txn *meb.StoreTxn) error {
			results, err := txn.SearchIVFPQ(context.Background(), topicID, query, 10)
			if err != nil {
				return fmt.Errorf("Search failed: %v", err)
			}
			count = len(results)
			return nil
		}); err != nil {
			log.Fatalf("View failed: %v", err)
		}
		if count == 0 {
			log.Fatal("expected results")
		}
		latencies = append(latencies, float64(time.Since(start).Microseconds())/1000.0)
	}

	sort.Float64s(latencies)

	totalTime := time.Duration(0)
	for _, l := range latencies {
		totalTime += time.Duration(l * float64(time.Millisecond))
	}
	throughput := float64(numTrials) / totalTime.Seconds()

	return ScenarioResult{
		Name:       fmt.Sprintf("IVFPQSearch_%dK", numVectors/1000),
		OpsPerSec:  throughput,
		P50Ms:      percentile(latencies, 50),
		P95Ms:      percentile(latencies, 95),
		P99Ms:      percentile(latencies, 99),
	}
}

func benchHNSWSearch(numVectors int) ScenarioResult {
	s, cleanup := newBenchStoreWithHNSW()
	defer cleanup()

	dim := 128
	rng := rand.New(rand.NewSource(42))

	topicID := uint32(1)

	vectors := make([][]float32, numVectors)
	// Add vectors via HNSW
	if err := s.Update(func(txn *meb.StoreTxn) error {
		for i := 0; i < numVectors; i++ {
			vec := randomUnitVector(rng, dim)
			vectors[i] = vec
			if err := txn.AddHNSWVector(topicID, uint64(i+1), vec); err != nil {
				return fmt.Errorf("AddHNSWVector failed: %v", err)
			}
		}
		return nil
	}); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	query := randomUnitVector(rng, dim)
	time.Sleep(100 * time.Millisecond)

	// Debug: search with first indexed vector
	if err := s.View(func(txn *meb.StoreTxn) error {
		results, err := txn.SearchHNSW(context.Background(), topicID, vectors[0], 10)
		if err != nil {
			return fmt.Errorf("Search failed: %v", err)
		}
		log.Printf("HNSW search with indexed vector returned %d results", len(results))
		return nil
	}); err != nil {
		log.Fatalf("Debug HNSW failed: %v", err)
	}

	const numTrials = 50
	var latencies []float64
	for i := 0; i < numTrials; i++ {
		start := time.Now()
		var count int
		if err := s.View(func(txn *meb.StoreTxn) error {
			results, err := txn.SearchHNSW(context.Background(), topicID, query, 10)
			if err != nil {
				return fmt.Errorf("Search failed: %v", err)
			}
			count = len(results)
			return nil
		}); err != nil {
			log.Fatalf("View failed: %v", err)
		}
		if count == 0 {
			log.Fatal("expected results")
		}
		latencies = append(latencies, float64(time.Since(start).Microseconds())/1000.0)
	}

	sort.Float64s(latencies)

	totalTime := time.Duration(0)
	for _, l := range latencies {
		totalTime += time.Duration(l * float64(time.Millisecond))
	}
	throughput := float64(numTrials) / totalTime.Seconds()

	return ScenarioResult{
		Name:       fmt.Sprintf("HNSWSearch_%dK", numVectors/1000),
		OpsPerSec:  throughput,
		P50Ms:      percentile(latencies, 50),
		P95Ms:      percentile(latencies, 95),
		P99Ms:      percentile(latencies, 99),
	}
}

func benchRecall() ScenarioResult {
	s, cleanup := newBenchStore()
	defer cleanup()

	dim := 128
	numVectors := 10000
	numQueries := 10
	rng := rand.New(rand.NewSource(42))

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		v := randomUnitVector(rng, dim)
		vectors[i] = v
		if err := s.Vectors().Add(uint64(i+1), v); err != nil {
			log.Fatalf("Add failed: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var totalRecall float64
	for qi := 0; qi < numQueries; qi++ {
		query := randomUnitVector(rng, dim)

		// Brute-force ground truth
		type scored struct {
			id    uint64
			score float32
		}
		all := make([]scored, numVectors)
		for i, v := range vectors {
			all[i] = scored{
				id:    uint64(i + 1),
				score: dotProduct(query, v),
			}
		}
		sort.Slice(all, func(i, j int) bool {
			return all[i].score > all[j].score
		})
		groundTruth := make(map[uint64]bool)
		for i := 0; i < 10; i++ {
			groundTruth[all[i].id] = true
		}

		found := make(map[uint64]bool)
		for sr := range s.Vectors().Search(context.Background(), query, 10) {
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

	return ScenarioResult{
		Name:       "Recall@10",
		RecallAt10: totalRecall / float64(numQueries),
	}
}

func randomUnitVector(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	var sum float32
	for _, val := range v {
		sum += val * val
	}
	norm := float32(math.Sqrt(float64(sum)))
	if norm > 0 {
		for i := range v {
			v[i] /= norm
		}
	}
	return v
}

func dotProduct(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(p)/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Ensure vector package import is used
var _ = vector.DefaultConfig
