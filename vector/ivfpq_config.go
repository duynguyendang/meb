package vector

import "fmt"

type IVFPQConfig struct {
	NumCentroids    int
	NumSubSpaces    int
	BitsPerSpace    int
	NProbe          int
	NSample         int
	BatchSize       int
	StoreRawVectors bool          // Opt-in: persist raw float32 vectors under 0x16 (saves ~6.14 GB/1M vecs when false)
	HybridCfg       *HybridConfig // Optional: matches VectorRegistry Hybrid quantization when reading 0x11
}

func DefaultIVFPQConfig() *IVFPQConfig {
	return &IVFPQConfig{
		NumCentroids:    1024,
		NumSubSpaces:    64,
		BitsPerSpace:    8,
		NProbe:          32,
		NSample:         100_000,
		BatchSize:       10_000,
		StoreRawVectors: false,
	}
}

func (c *IVFPQConfig) Validate() error {
	if c.NumCentroids < 1 || c.NumCentroids > 65536 {
		return fmt.Errorf("NumCentroids must be 1-65536, got %d", c.NumCentroids)
	}
	if c.NumSubSpaces < 1 || c.NumSubSpaces > 256 {
		return fmt.Errorf("NumSubSpaces must be 1-256, got %d", c.NumSubSpaces)
	}
	if c.BitsPerSpace != 8 {
		return fmt.Errorf("BitsPerSpace must be 8, got %d", c.BitsPerSpace)
	}
	if c.NProbe < 1 {
		return fmt.Errorf("NProbe must be >= 1, got %d", c.NProbe)
	}
	return nil
}
