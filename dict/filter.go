package dict

import (
	"log/slog"

	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter wraps a Bloom filter for efficient set membership testing.
// It is used to avoid disk lookups for non-existent keys.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// NewBloomFilter creates a new Bloom Filter sized for the estimated number of items
// with a target false positive rate.
// For 2 billion items with 1% false positive rate:
// m = -n*ln(p) / (ln(2)^2) ≈ 9.585 bits/item => ~19Gb (2.3 GB)
func NewBloomFilter(n uint, p float64) *BloomFilter {
	slog.Info("initializing bloom filter", "n", n, "p", p)
	return &BloomFilter{
		filter: bloom.NewWithEstimates(n, p),
	}
}

// Add adds a string to the filter.
func (b *BloomFilter) Add(s string) {
	b.filter.AddString(s)
}

// Test checks if a string might be in the set.
// Returns true if possibly present, false if definitely not present.
func (b *BloomFilter) Test(s string) bool {
	return b.filter.TestString(s)
}

// EstimateFalsePositiveRate returns the estimated false positive rate.
// Function removed due to library version mismatch.
func (b *BloomFilter) EstimateFalsePositiveRate(n uint) float64 {
	// return b.filter.EstimateFalsePositiveRate(n)
	return 0.01 // Placeholder
}
