package vector

import (
	"encoding/binary"
	"math"
)

const (
	DefaultTQBitWidth  = 8
	DefaultTQBlockSize = 32
)

type TurboQuantConfig struct {
	BitWidth  int
	BlockSize int
}

func DefaultTurboQuantConfig() *TurboQuantConfig {
	return &TurboQuantConfig{
		BitWidth:  DefaultTQBitWidth,
		BlockSize: DefaultTQBlockSize,
	}
}

func TQVectorSize(dim int, cfg *TurboQuantConfig) int {
	if cfg == nil {
		cfg = DefaultTurboQuantConfig()
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

// QuantizeTurboQuant compresses a float32 vector to TurboQuant format.
// Format per block: [scale:4 LE][zero:4 LE][q_0:1][q_1:1]...[q_{B-1}:1]
func QuantizeTurboQuant(vec []float32, cfg *TurboQuantConfig) []byte {
	if cfg == nil {
		cfg = DefaultTurboQuantConfig()
	}

	dim := len(vec)
	blockSize := cfg.BlockSize
	numBlocks := (dim + blockSize - 1) / blockSize
	outSize := TQVectorSize(dim, cfg)
	out := make([]byte, outSize)

	offset := 0
	for b := 0; b < numBlocks; b++ {
		start := b * blockSize
		end := start + blockSize
		if end > dim {
			end = dim
		}

		minVal, maxVal := float32(math.MaxFloat32), float32(-math.MaxFloat32)
		for i := start; i < end; i++ {
			if vec[i] < minVal {
				minVal = vec[i]
			}
			if vec[i] > maxVal {
				maxVal = vec[i]
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
				v := (vec[i] - zero) / scale
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
				v1 := (vec[i] - zero) / scale
				q1 := int(math.Round(float64(v1)))
				if q1 < 0 {
					q1 = 0
				} else if q1 > 15 {
					q1 = 15
				}
				var q2 int
				if i+1 < end {
					v2 := (vec[i+1] - zero) / scale
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

// DequantizeTurboQuant decompresses a TQ vector back to float32.
func DequantizeTurboQuant(data []byte, dim int, cfg *TurboQuantConfig) []float32 {
	if cfg == nil {
		cfg = DefaultTurboQuantConfig()
	}

	vec := make([]float32, dim)
	blockSize := cfg.BlockSize
	numBlocks := (dim + blockSize - 1) / blockSize

	offset := 0
	for b := 0; b < numBlocks; b++ {
		start := b * blockSize
		end := start + blockSize
		if end > dim {
			end = dim
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
				b := data[offset]
				vec[i] = float32(b>>4)*scale + zero
				if i+1 < end {
					vec[i+1] = float32(b&0x0F)*scale + zero
				}
				offset++
			}
			offset += blockSize/2 - (end-start+1)/2
		}
	}

	return vec
}

// DotProductTurboQuant computes cosine similarity between two TQ-compressed vectors
// using blockwise dot product without full dequantization.
func DotProductTurboQuant(a, b []byte, dim int, cfg *TurboQuantConfig) float32 {
	if cfg == nil {
		cfg = DefaultTurboQuantConfig()
	}

	blockSize := cfg.BlockSize
	numBlocks := (dim + blockSize - 1) / blockSize

	var totalSum float32
	offsetA := 0
	offsetB := 0

	for block := 0; block < numBlocks; block++ {
		blockLen := blockSize
		if (block+1)*blockSize > dim {
			blockLen = dim - block*blockSize
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
			for i := 0; i < blockLen; i++ {
				qa := int64(a[offsetA+i])
				qb := int64(b[offsetB+i])
				sumQQ += qa * qb
				sumQA += qa
				sumQB += qb
			}
			offsetA += blockSize
			offsetB += blockSize
		case 4:
			for i := 0; i < blockLen; i += 2 {
				byteA := a[offsetA+i/2]
				byteB := b[offsetB+i/2]
				qa1 := int64(byteA >> 4)
				qb1 := int64(byteB >> 4)
				sumQQ += qa1 * qb1
				sumQA += qa1
				sumQB += qb1
				if i+1 < blockLen {
					qa2 := int64(byteA & 0x0F)
					qb2 := int64(byteB & 0x0F)
					sumQQ += qa2 * qb2
					sumQA += qa2
					sumQB += qb2
				}
			}
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

// DotProductTQFull dequantizes both vectors and computes exact dot product.
func DotProductTQFull(a, b []byte, dim int, cfg *TurboQuantConfig) float32 {
	vecA := DequantizeTurboQuant(a, dim, cfg)
	vecB := DequantizeTurboQuant(b, dim, cfg)
	return DotProduct(vecA, vecB)
}
