package vector

type DiskANNConfig struct {
	R         int
	L         int
	Alpha     float64
	CacheSize int
}

type DiskANNIndex struct {
	cfg       *DiskANNConfig
	fullDim   int
	paddedDim int
}

func NewDiskANNIndex(cfg *DiskANNConfig, fullDim int) *DiskANNIndex {
	paddedDim := nextPow2(fullDim)
	return &DiskANNIndex{
		cfg:       cfg,
		fullDim:   fullDim,
		paddedDim: paddedDim,
	}
}
