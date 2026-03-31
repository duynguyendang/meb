package vector

// FWHT performs the in-place Fast Walsh-Hadamard Transform.
// Unnormalized: applying FWHT twice yields N * original.
// Inverse is FWHT followed by division by N.
func FWHT(vec []float32) {
	n := len(vec)
	for h := 1; h < n; h <<= 1 {
		for i := 0; i < n; i += h << 1 {
			for j := 0; j < h; j++ {
				x := vec[i+j]
				y := vec[i+j+h]
				vec[i+j] = x + y
				vec[i+j+h] = x - y
			}
		}
	}
}

// nextPow2 returns the smallest power of 2 >= n.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
