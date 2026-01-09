package vector

import (
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// BenchmarkVectorRegistry_Int8Search benchmarks the int8 search implementation.
func BenchmarkVectorRegistry_Int8Search(b *testing.B) {
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	reg := NewRegistry(db)

	// Add 100k vectors
	for i := 0; i < 100000; i++ {
		vec := generateFullRandomVector()
		_ = reg.Add(uint64(i), vec)
	}
	reg.wg.Wait()

	queryVec := generateFullRandomVector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Search(queryVec, 10)
	}
}

// BenchmarkMemoryUsage_Int8 compares memory usage between int8 and float32.
// This test shows that int8 uses 75% less memory.
func BenchmarkMemoryUsage_Int8(b *testing.B) {
	// Simulate storing 1M vectors
	numVectors := 1000000

	b.Run("int8", func(b *testing.B) {
		data := make([]int8, numVectors*MRLDim)
		b.ReportMetric(float64(len(data))/1024/1024, "MB")
	})

	b.Run("float32", func(b *testing.B) {
		data := make([]float32, numVectors*MRLDim)
		b.ReportMetric(float64(len(data)*4)/1024/1024, "MB")
	})
}

// generateFullRandomVector generates a random 1536-d vector.
func generateFullRandomVector() []float32 {
	vec := make([]float32, FullDim)
	for i := range vec {
		vec[i] = rand.Float32()*2 - 1
	}
	return vec
}

func init() {
	// Seed for reproducibility
	rand.Seed(42)
}
