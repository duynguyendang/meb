package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/duynguyendang/meb/vector"
)

// Doc represents a synthetic document for stress testing
type Doc struct {
	ID      string
	Vector  []float32
	Content []byte
	Links   []string // IDs this doc points to (Graph connections)
}

// DataGenerator generates synthetic test data
type DataGenerator struct {
	randPool [][]float32
	poolSize int
	rng      *rand.Rand
}

// NewDataGenerator creates a new data generator with pre-allocated vector pool
func NewDataGenerator() *DataGenerator {
	// Pre-generate a pool of random vectors to avoid calling rand.Float32 repeatedly
	poolSize := 10000
	randPool := make([][]float32, poolSize)
	src := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(src)

	for i := 0; i < poolSize; i++ {
		randPool[i] = randomFullVector(rng)
	}

	return &DataGenerator{
		randPool: randPool,
		poolSize: poolSize,
		rng:      rng,
	}
}

// RandomFullVector returns a random full-dimension vector (1536-d)
// Uses pre-allocated pool when possible
func (g *DataGenerator) RandomFullVector() []float32 {
	// Pick from pool
	idx := g.rng.Intn(g.poolSize)
	vec := make([]float32, vector.FullDim)
	copy(vec, g.randPool[idx])
	return vec
}

// GenerateBatch creates 'n' documents starting from startID
// Uses pre-allocated buffers for efficiency
func (g *DataGenerator) GenerateBatch(startID int, count int) []Doc {
	docs := make([]Doc, count)

	for i := 0; i < count; i++ {
		id := startID + i
		docID := fmt.Sprintf("doc_%d", id)

		// Get random vector from pool
		vec := g.RandomFullVector()

		// Generate S2-compressible text content (~500 bytes)
		content := randomText(500)

		// Generate random links (0-3 links per doc)
		numLinks := g.rng.Intn(4)
		links := make([]string, 0, numLinks)
		for j := 0; j < numLinks; j++ {
			// Link to a random document (could be before or after current doc)
			linkID := g.rng.Intn(startID + count + 1000) // +1000 allows links to future docs
			links = append(links, fmt.Sprintf("doc_%d", linkID))
		}

		docs[i] = Doc{
			ID:      docID,
			Vector:  vec,
			Content: content,
			Links:   links,
		}
	}

	return docs
}

// randomFullVector generates a random 1536-d vector
func randomFullVector(rng *rand.Rand) []float32 {
	vec := make([]float32, vector.FullDim)
	for i := range vec {
		vec[i] = rng.Float32()*2 - 1 // [-1, 1]
	}
	return vec
}

// randomText generates S2-compressible synthetic text
// Uses repetitive patterns for better compression
func randomText(size int) []byte {
	// Use a set of reusable phrases for better S2 compression
	phrases := []string{
		"The quick brown fox jumps over the lazy dog. ",
		"Pack my box with five dozen liquor jugs. ",
		"How vexingly quick daft zebras jump! ",
		"Sphinx of black quartz, judge my vow. ",
		"Two driven jocks help fax my big quiz. ",
		"The five boxing wizards jump quickly. ",
	}

	var result []byte
	for len(result) < size {
		// Pick random phrase
		phrase := phrases[rand.Intn(len(phrases))]
		result = append(result, phrase...)
	}

	// Trim to exact size
	if len(result) > size {
		result = result[:size]
	}

	return result
}
