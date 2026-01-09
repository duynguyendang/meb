package vector

// DotProductInt8 computes the approximate dot product of two int8 vectors.
// Since the vectors are scalar-quantized from normalized floats, we can:
// 1. Multiply the int8 values directly (fast)
// 2. Scale down by 127*127 = 16129 to get the approximate similarity
//
// This provides ~3x speedup over float32 due to:
// - Smaller data size (better cache utilization)
// - Faster integer arithmetic
// - Reduced memory bandwidth
func DotProductInt8(v1, v2 []int8) float32 {
	// Bounds check elimination
	_ = v1[63]
	_ = v2[63]

	var sum int32

	// Unroll by 8 for better instruction-level parallelism
	sum = int32(v1[0])*int32(v2[0]) + int32(v1[1])*int32(v2[1]) +
		int32(v1[2])*int32(v2[2]) + int32(v1[3])*int32(v2[3]) +
		int32(v1[4])*int32(v2[4]) + int32(v1[5])*int32(v2[5]) +
		int32(v1[6])*int32(v2[6]) + int32(v1[7])*int32(v2[7])

	sum += int32(v1[8])*int32(v2[8]) + int32(v1[9])*int32(v2[9]) +
		int32(v1[10])*int32(v2[10]) + int32(v1[11])*int32(v2[11]) +
		int32(v1[12])*int32(v2[12]) + int32(v1[13])*int32(v2[13]) +
		int32(v1[14])*int32(v2[14]) + int32(v1[15])*int32(v2[15])

	sum += int32(v1[16])*int32(v2[16]) + int32(v1[17])*int32(v2[17]) +
		int32(v1[18])*int32(v2[18]) + int32(v1[19])*int32(v2[19]) +
		int32(v1[20])*int32(v2[20]) + int32(v1[21])*int32(v2[21]) +
		int32(v1[22])*int32(v2[22]) + int32(v1[23])*int32(v2[23])

	sum += int32(v1[24])*int32(v2[24]) + int32(v1[25])*int32(v2[25]) +
		int32(v1[26])*int32(v2[26]) + int32(v1[27])*int32(v2[27]) +
		int32(v1[28])*int32(v2[28]) + int32(v1[29])*int32(v2[29]) +
		int32(v1[30])*int32(v2[30]) + int32(v1[31])*int32(v2[31])

	sum += int32(v1[32])*int32(v2[32]) + int32(v1[33])*int32(v2[33]) +
		int32(v1[34])*int32(v2[34]) + int32(v1[35])*int32(v2[35]) +
		int32(v1[36])*int32(v2[36]) + int32(v1[37])*int32(v2[37]) +
		int32(v1[38])*int32(v2[38]) + int32(v1[39])*int32(v2[39])

	sum += int32(v1[40])*int32(v2[40]) + int32(v1[41])*int32(v2[41]) +
		int32(v1[42])*int32(v2[42]) + int32(v1[43])*int32(v2[43]) +
		int32(v1[44])*int32(v2[44]) + int32(v1[45])*int32(v2[45]) +
		int32(v1[46])*int32(v2[46]) + int32(v1[47])*int32(v2[47])

	sum += int32(v1[48])*int32(v2[48]) + int32(v1[49])*int32(v2[49]) +
		int32(v1[50])*int32(v2[50]) + int32(v1[51])*int32(v2[51]) +
		int32(v1[52])*int32(v2[52]) + int32(v1[53])*int32(v2[53]) +
		int32(v1[54])*int32(v2[54]) + int32(v1[55])*int32(v2[55])

	sum += int32(v1[56])*int32(v2[56]) + int32(v1[57])*int32(v2[57]) +
		int32(v1[58])*int32(v2[58]) + int32(v1[59])*int32(v2[59]) +
		int32(v1[60])*int32(v2[60]) + int32(v1[61])*int32(v2[61]) +
		int32(v1[62])*int32(v2[62]) + int32(v1[63])*int32(v2[63])

	// Scale back to [-1, 1] range
	// Since both vectors were normalized and quantized with factor 127,
	// the dot product is scaled by 127*127 = 16129
	return float32(sum) / 16129.0
}
