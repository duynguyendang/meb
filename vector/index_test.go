package vector

import "testing"

func TestSelectIndexAutoThresholds(t *testing.T) {
	tests := []struct {
		name        string
		corpusSize  int
		preferHNSW  bool
		want        ANNIndexType
		description string
	}{
		{
			name:        "small_corpus_brute_force",
			corpusSize:  100_000,
			preferHNSW:  false,
			want:        IndexBruteForce,
			description: "Corpus < 500K should use brute force",
		},
		{
			name:        "medium_corpus_auto_hnsw",
			corpusSize:  750_000,
			preferHNSW:  false,
			want:        IndexHNSW,
			description: "Corpus 500K-1M should auto-select HNSW",
		},
		{
			name:        "threshold_auto_hnsw",
			corpusSize:  999_999,
			preferHNSW:  false,
			want:        IndexHNSW,
			description: "Corpus just below 1M threshold should auto-select HNSW",
		},
		{
			name:        "large_corpus_ivfpq",
			corpusSize:  1_000_000,
			preferHNSW:  false,
			want:        IndexIVFPQ,
			description: "Corpus >= 1M should use IVF-PQ by default",
		},
		{
			name:        "large_corpus_prefer_hnsw",
			corpusSize:  2_000_000,
			preferHNSW:  true,
			want:        IndexHNSW,
			description: "Corpus 1M-10M with preferHNSW should use HNSW",
		},
		{
			name:        "very_large_corpus_ivfpq",
			corpusSize:  10_000_000,
			preferHNSW:  false,
			want:        IndexIVFPQ,
			description: "Corpus >= 10M should use IVF-PQ",
		},
		{
			name:        "very_large_corpus_prefer_hnsw",
			corpusSize:  10_000_000,
			preferHNSW:  true,
			want:        IndexIVFPQ,
			description: "Corpus >= 10M should use IVF-PQ even with preferHNSW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectIndex(tt.corpusSize, tt.preferHNSW)
			if got != tt.want {
				t.Errorf("%s: SelectIndex(%d, %v) = %v, want %v",
					tt.description, tt.corpusSize, tt.preferHNSW, got, tt.want)
			}
		})
	}
}
