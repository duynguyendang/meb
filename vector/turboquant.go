package vector

import (
	"encoding/binary"
	"math"
)

const (
	DefaultHybridBitWidth  = 8
	DefaultHybridBlockSize = 32
)

type HybridConfig struct {
	BitWidth  int
	BlockSize int
	// Cached derived values (set by NewHybridConfig)
	paddedDim int
	numBlocks int
}

func NewHybridConfig(bitWidth, blockSize int) *HybridConfig {
	return &HybridConfig{
		BitWidth:  bitWidth,
		BlockSize: blockSize,
	}
}

// Ensure paddedDim/numBlocks are computed for a given vector dimension.
// Called before hot-path operations (DotProductHybrid, QuantizeHybrid).
func (c *HybridConfig) ensureDerived(dim int) {
	pd := nextPow2(dim)
	if c.paddedDim == pd {
		return
	}
	c.paddedDim = pd
	c.numBlocks = (pd + c.BlockSize - 1) / c.BlockSize
}

func DefaultHybridConfig() *HybridConfig {
	return &HybridConfig{
		BitWidth:  DefaultHybridBitWidth,
		BlockSize: DefaultHybridBlockSize,
	}
}

func HybridVectorSize(dim int, cfg *HybridConfig) int {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}
	numBlocks := (dim + cfg.BlockSize - 1) / cfg.BlockSize
	// Per block: [scale:4][zero:4][norm:4][quantized:N]
	switch cfg.BitWidth {
	case 8:
		return numBlocks * (12 + cfg.BlockSize)
	case 4:
		return numBlocks * (12 + cfg.BlockSize/2)
	default:
		return numBlocks * (12 + cfg.BlockSize)
	}
}

func QuantizeHybrid(vec []float32, cfg *HybridConfig) []byte {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}

	dim := len(vec)
	cfg.ensureDerived(dim)
	paddedDim := cfg.paddedDim

	padded := make([]float32, paddedDim)
	copy(padded, vec)

	FWHT(padded)

	invNorm := 1.0 / float32(math.Sqrt(float64(paddedDim)))
	for i := range padded {
		padded[i] *= invNorm
	}

	blockSize := cfg.BlockSize
	numBlocks := cfg.numBlocks
	outSize := HybridVectorSize(paddedDim, cfg)
	out := make([]byte, outSize)

	offset := 0
	for b := 0; b < numBlocks; b++ {
		start := b * blockSize
		end := start + blockSize
		if end > paddedDim {
			end = paddedDim
		}

		minVal, maxVal := float32(math.MaxFloat32), float32(-math.MaxFloat32)
		for i := start; i < end; i++ {
			if padded[i] < minVal {
				minVal = padded[i]
			}
			if padded[i] > maxVal {
				maxVal = padded[i]
			}
		}

		var scale, zero float32
		if maxVal == minVal {
			scale = 1.0
			zero = minVal
		} else {
			scale = (maxVal - minVal) / float32(int(1)<<cfg.BitWidth-1)
			zero = minVal
		}

		binary.LittleEndian.PutUint32(out[offset:], math.Float32bits(scale))
		offset += 4
		binary.LittleEndian.PutUint32(out[offset:], math.Float32bits(zero))
		offset += 4

		// Per-block L2 norm (pre-FWHT, before scaling) for Cauchy-Schwarz pruning
		var blockNormSq float64
		for i := start; i < end; i++ {
			blockNormSq += float64(padded[i]) * float64(padded[i])
		}
		binary.LittleEndian.PutUint32(out[offset:], math.Float32bits(float32(math.Sqrt(blockNormSq))))
		offset += 4

		switch cfg.BitWidth {
		case 8:
			for i := start; i < end; i++ {
				v := (padded[i] - zero) / scale
				q := int(math.Round(float64(v)))
				if q < 0 {
					q = 0
				} else if q > 255 {
					q = 255
				}
				out[offset] = byte(q)
				offset++
			}
			for i := end - start; i < blockSize; i++ {
				out[offset] = 0
				offset++
			}
		case 4:
			for i := start; i < end; i += 2 {
				v1 := (padded[i] - zero) / scale
				q1 := int(math.Round(float64(v1)))
				if q1 < 0 {
					q1 = 0
				} else if q1 > 15 {
					q1 = 15
				}
				var q2 int
				if i+1 < end {
					v2 := (padded[i+1] - zero) / scale
					q2 = int(math.Round(float64(v2)))
					if q2 < 0 {
						q2 = 0
					} else if q2 > 15 {
						q2 = 15
					}
				}
				out[offset] = byte(q1<<4 | q2)
				offset++
			}
			for i := (end - start + 1) / 2; i < blockSize/2; i++ {
				out[offset] = 0
				offset++
			}
		}
	}

	return out
}

func DequantizeHybrid(data []byte, dim int, cfg *HybridConfig) []float32 {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}

	cfg.ensureDerived(dim)
	paddedDim := cfg.paddedDim
	numBlocks := cfg.numBlocks

	vec := make([]float32, paddedDim)
	blockSize := cfg.BlockSize

	offset := 0
	for b := 0; b < numBlocks; b++ {
		start := b * blockSize
		end := start + blockSize
		if end > paddedDim {
			end = paddedDim
		}

		scaleBits := binary.LittleEndian.Uint32(data[offset:])
		scale := math.Float32frombits(scaleBits)
		offset += 4
		zeroBits := binary.LittleEndian.Uint32(data[offset:])
		zero := math.Float32frombits(zeroBits)
		offset += 4
		// Skip per-block norm (4 bytes)
		offset += 4

		switch cfg.BitWidth {
		case 8:
			for i := start; i < end; i++ {
				vec[i] = float32(data[offset])*scale + zero
				offset++
			}
			offset += blockSize - (end - start)
		case 4:
			for i := start; i < end; i += 2 {
				packed := data[offset]
				vec[i] = float32(packed>>4)*scale + zero
				if i+1 < end {
					vec[i+1] = float32(packed&0x0F)*scale + zero
				}
				offset++
			}
			offset += blockSize/2 - (end-start+1)/2
		}
	}

	FWHT(vec)
	invNorm := 1.0 / float32(math.Sqrt(float64(paddedDim)))
	for i := range vec {
		vec[i] *= invNorm
	}

	result := make([]float32, dim)
	copy(result, vec[:dim])
	return result
}

func DotProductHybrid(a, b []byte, dim int, cfg *HybridConfig) float32 {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}

	cfg.ensureDerived(dim)
	paddedDim := cfg.paddedDim
	numBlocks := cfg.numBlocks
	expectedSize := HybridVectorSize(paddedDim, cfg)
	if len(a) < expectedSize || len(b) < expectedSize {
		return 0
	}

	blockSize := cfg.BlockSize

	var totalSum float32
	offsetA := 0
	offsetB := 0

	for block := 0; block < numBlocks; block++ {
		blockLen := blockSize
		if (block+1)*blockSize > paddedDim {
			blockLen = paddedDim - block*blockSize
		}

		scaleA := math.Float32frombits(binary.LittleEndian.Uint32(a[offsetA:]))
		zeroA := math.Float32frombits(binary.LittleEndian.Uint32(a[offsetA+4:]))
		scaleB := math.Float32frombits(binary.LittleEndian.Uint32(b[offsetB:]))
		zeroB := math.Float32frombits(binary.LittleEndian.Uint32(b[offsetB+4:]))

		// Skip scale(4) + zero(4) + norm(4) = 12 bytes
		offsetA += 12
		offsetB += 12

		var sumQQ, sumQA, sumQB int64

		switch cfg.BitWidth {
		case 8:
			sumQQ, sumQA, sumQB = dotProdBlock8(a[offsetA:offsetA+blockLen], b[offsetB:offsetB+blockLen])
			offsetA += blockSize
			offsetB += blockSize
		case 4:
			sumQQ, sumQA, sumQB = dotProdBlock4(a[offsetA:offsetA+blockLen/2], b[offsetB:offsetB+blockLen/2], blockLen)
			offsetA += blockSize / 2
			offsetB += blockSize / 2
		}

		totalSum += scaleA*scaleB*float32(sumQQ) +
			scaleA*zeroB*float32(sumQA) +
			scaleB*zeroA*float32(sumQB) +
			float32(blockLen)*zeroA*zeroB
	}

	return totalSum
}

func DotProductHybridFull(a, b []byte, dim int, cfg *HybridConfig) float32 {
	vecA := DequantizeHybrid(a, dim, cfg)
	vecB := DequantizeHybrid(b, dim, cfg)
	return DotProduct(vecA, vecB)
}

// ComputeQueryBlockNorms extracts per-block L2 norms from a quantized query vector.
// These are precomputed once per query and reused across all database vectors in the scan.
func ComputeQueryBlockNorms(query []byte, dim int, cfg *HybridConfig) []float32 {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}
	cfg.ensureDerived(dim)
	numBlocks := cfg.numBlocks

	norms := make([]float32, numBlocks)
	for b := 0; b < numBlocks; b++ {
		blockStride := 12 + cfg.BlockSize // scale(4) + zero(4) + norm(4) + quantized
		if cfg.BitWidth == 4 {
			blockStride = 12 + cfg.BlockSize/2
		}
		off := b*blockStride + 8 // skip scale(4) + zero(4), read norm(4)
		if off+4 <= len(query) {
			norms[b] = math.Float32frombits(binary.LittleEndian.Uint32(query[off:]))
		}
	}
	return norms
}

// DotProductHybridWithPruning computes the hybrid dot product with Cauchy-Schwarz
// early termination. For each database vector, it reads per-block norms, computes
// a suffix sum of norm products, and skips remaining blocks when it's impossible
// to beat the current threshold.
//
// queryBlockNorms is precomputed via ComputeQueryBlockNorms.
// threshold is the current kth-best score (pass 0 for no pruning).
func DotProductHybridWithPruning(a, b []byte, dim int, cfg *HybridConfig, threshold float32, queryBlockNorms []float32) float32 {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}

	cfg.ensureDerived(dim)
	paddedDim := cfg.paddedDim
	numBlocks := cfg.numBlocks
	expectedSize := HybridVectorSize(paddedDim, cfg)
	if len(a) < expectedSize || len(b) < expectedSize {
		return 0
	}

	blockSize := cfg.BlockSize

	// Read per-block norms from the database vector into a stack-sized array.
	// 64 blocks × 4 bytes = 256 bytes fits easily on stack.
	var normA [64]float32
	for bl := 0; bl < numBlocks; bl++ {
		blockStride := 12 + cfg.BlockSize
		if cfg.BitWidth == 4 {
			blockStride = 12 + cfg.BlockSize/2
		}
		off := bl*blockStride + 8 // skip scale(4) + zero(4), read norm(4)
		if off+4 <= len(a) {
			normA[bl] = math.Float32frombits(binary.LittleEndian.Uint32(a[off:]))
		}
	}

	// Compute suffix sums of normA[r] * queryBlockNorms[r] for Cauchy-Schwarz bound.
	// suffixNormSum[b] = Σ_{r>=b} normA[r] * queryBlockNorms[r]
	var suffixNormSum [65]float32
	for bl := numBlocks - 1; bl >= 0; bl-- {
		suffixNormSum[bl] = suffixNormSum[bl+1] + normA[bl]*queryBlockNorms[bl]
	}

	var totalSum float32
	offsetA := 0
	offsetB := 0

	for block := 0; block < numBlocks; block++ {
		blockLen := blockSize
		if (block+1)*blockSize > paddedDim {
			blockLen = paddedDim - block*blockSize
		}

		scaleA := math.Float32frombits(binary.LittleEndian.Uint32(a[offsetA:]))
		zeroA := math.Float32frombits(binary.LittleEndian.Uint32(a[offsetA+4:]))
		scaleB := math.Float32frombits(binary.LittleEndian.Uint32(b[offsetB:]))
		zeroB := math.Float32frombits(binary.LittleEndian.Uint32(b[offsetB+4:]))

		offsetA += 12
		offsetB += 12

		var sumQQ, sumQA, sumQB int64

		switch cfg.BitWidth {
		case 8:
			sumQQ, sumQA, sumQB = dotProdBlock8(a[offsetA:offsetA+blockLen], b[offsetB:offsetB+blockLen])
			offsetA += blockSize
			offsetB += blockSize
		case 4:
			sumQQ, sumQA, sumQB = dotProdBlock4(a[offsetA:offsetA+blockLen/2], b[offsetB:offsetB+blockLen/2], blockLen)
			offsetA += blockSize / 2
			offsetB += blockSize / 2
		}

		totalSum += scaleA*scaleB*float32(sumQQ) +
			scaleA*zeroB*float32(sumQA) +
			scaleB*zeroA*float32(sumQB) +
			float32(blockLen)*zeroA*zeroB

		// Early termination: if maximum possible contribution from remaining blocks
		// cannot beat the threshold, stop immediately.
		if threshold > 0 && block < numBlocks-1 {
			if totalSum+suffixNormSum[block+1] < threshold {
				return totalSum
			}
		}
	}

	return totalSum
}
