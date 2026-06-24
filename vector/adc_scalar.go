package vector

func adcAccumImplScalar(lut []float32, codes []byte) float32 {
	var dist float32
	for s := 0; s < len(codes); s++ {
		dist += lut[s*256+int(codes[s])]
	}
	return dist
}
