package vector

import "math"

func Quantize(vec []float32) []int8 {
	res := make([]int8, len(vec))
	for i, v := range vec {
		if v > 1.0 {
			v = 1.0
		} else if v < -1.0 {
			v = -1.0
		}
		res[i] = int8(math.Round(float64(v * 127)))
	}
	return res
}

func Dequantize(vec []int8) []float32 {
	res := make([]float32, len(vec))
	for i, v := range vec {
		res[i] = float32(v) / 127.0
	}
	return res
}
