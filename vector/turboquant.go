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
	switch cfg.BitWidth {
	case 8:
		return numBlocks * (8 + cfg.BlockSize)
	case 4:
		return numBlocks * (8 + cfg.BlockSize/2)
	default:
		return numBlocks * (8 + cfg.BlockSize)
	}
}

func QuantizeHybrid(vec []float32, cfg *HybridConfig) []byte {
	if cfg == nil {
		cfg = DefaultHybridConfig()
	}

	dim := len(vec)
	paddedDim := nextPow2(dim)

	padded := make([]float32, paddedDim)
	copy(padded, vec)

	FWHT(padded)

	invNorm := 1.0 / float32(math.Sqrt(float64(paddedDim)))
	for i := range padded {
		padded[i] *= invNorm
	}

	blockSize := cfg.BlockSize
	numBlocks := (paddedDim + blockSize - 1) / blockSize
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

	paddedDim := nextPow2(dim)
	vec := make([]float32, paddedDim)
	blockSize := cfg.BlockSize
	numBlocks := (paddedDim + blockSize - 1) / blockSize

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

	paddedDim := nextPow2(dim)
	expectedSize := HybridVectorSize(paddedDim, cfg)
	if len(a) < expectedSize || len(b) < expectedSize {
		return 0
	}

	blockSize := cfg.BlockSize
	numBlocks := (paddedDim + blockSize - 1) / blockSize

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

		offsetA += 8
		offsetB += 8

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
