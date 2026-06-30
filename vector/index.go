package vector

const (
	BruteForceMaxN = 500_000
	HNSWMaxN       = 10_000_000
	// HNSWAutoSelectThreshold is the corpus size below which HNSW is auto-selected
	// for better recall. Above this, IVF-PQ is preferred for scalability.
	HNSWAutoSelectThreshold = 1_000_000
)

type ANNIndexType int

const (
	IndexBruteForce ANNIndexType = iota
	IndexIVFPQ
	IndexHNSW
	IndexDiskANN
)

// SelectIndex chooses the best ANN index type based on corpus size.
// HNSW is auto-selected for corpus sizes between BruteForceMaxN and HNSWAutoSelectThreshold
// to provide better recall without requiring explicit opt-in.
// The preferHNSW parameter provides an explicit override to force HNSW selection.
func SelectIndex(corpusSize int, preferHNSW bool) ANNIndexType {
	if corpusSize < BruteForceMaxN {
		return IndexBruteForce
	}
	// Auto-select HNSW for small-to-medium datasets (better recall)
	// or if explicitly requested via preferHNSW
	if corpusSize < HNSWAutoSelectThreshold || (preferHNSW && corpusSize < HNSWMaxN) {
		return IndexHNSW
	}
	return IndexIVFPQ
}
