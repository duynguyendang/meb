package vector

// adcAccumScalar is the portable scalar ADC accumulator.
// Computes Σ_s lut[s*256 + codes[s]] using 4-way unrolled independent
// accumulators to expose instruction-level parallelism to the CPU pipeline.
func adcAccumScalar(lut []float32, codes []byte) float32 {
	n := len(codes)
	if n == 0 {
		return 0
	}

	// 4-way unroll with independent accumulators for ILP.
	var acc0, acc1, acc2, acc3 float32
	i := 0
	for ; i+3 < n; i += 4 {
		s0 := i
		s1 := i + 1
		s2 := i + 2
		s3 := i + 3
		acc0 += lut[s0*256+int(codes[s0])]
		acc1 += lut[s1*256+int(codes[s1])]
		acc2 += lut[s2*256+int(codes[s2])]
		acc3 += lut[s3*256+int(codes[s3])]
	}
	dist := acc0 + acc1 + acc2 + acc3
	for ; i < n; i++ {
		dist += lut[i*256+int(codes[i])]
	}
	return dist
}
