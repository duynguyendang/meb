package vector

const (
	BruteForceMaxN = 500_000
	HNSWMaxN       = 10_000_000
)

type ANNIndexType int

const (
	IndexBruteForce ANNIndexType = iota
	IndexIVFPQ
	IndexHNSW
	IndexDiskANN
)

func SelectIndex(corpusSize int, preferHNSW bool) ANNIndexType {
	if corpusSize < BruteForceMaxN {
		return IndexBruteForce
	}
	if corpusSize < HNSWMaxN && preferHNSW {
		return IndexHNSW
	}
	return IndexIVFPQ
}
