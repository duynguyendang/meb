package vector

// dotProdBlock8 computes the block-wise dot product for 8-bit quantized vectors.
// Uses manual loop unrolling (by 8) to help the Go compiler generate AVX2 instructions.
// Block size is 32 elements (DefaultHybridBlockSize).
func dotProdBlock8(a, b []byte) (sumQQ, sumQA, sumQB int64) {
	// Bounds check elimination
	_ = a[31]
	_ = b[31]

	// Unroll by 8, 4 groups for block size 32
	qa0 := int64(a[0])
	qb0 := int64(b[0])
	qa1 := int64(a[1])
	qb1 := int64(b[1])
	qa2 := int64(a[2])
	qb2 := int64(b[2])
	qa3 := int64(a[3])
	qb3 := int64(b[3])
	qa4 := int64(a[4])
	qb4 := int64(b[4])
	qa5 := int64(a[5])
	qb5 := int64(b[5])
	qa6 := int64(a[6])
	qb6 := int64(b[6])
	qa7 := int64(a[7])
	qb7 := int64(b[7])
	sumQQ = qa0*qb0 + qa1*qb1 + qa2*qb2 + qa3*qb3 + qa4*qb4 + qa5*qb5 + qa6*qb6 + qa7*qb7
	sumQA = qa0 + qa1 + qa2 + qa3 + qa4 + qa5 + qa6 + qa7
	sumQB = qb0 + qb1 + qb2 + qb3 + qb4 + qb5 + qb6 + qb7

	qa8 := int64(a[8])
	qb8 := int64(b[8])
	qa9 := int64(a[9])
	qb9 := int64(b[9])
	qa10 := int64(a[10])
	qb10 := int64(b[10])
	qa11 := int64(a[11])
	qb11 := int64(b[11])
	qa12 := int64(a[12])
	qb12 := int64(b[12])
	qa13 := int64(a[13])
	qb13 := int64(b[13])
	qa14 := int64(a[14])
	qb14 := int64(b[14])
	qa15 := int64(a[15])
	qb15 := int64(b[15])
	sumQQ += qa8*qb8 + qa9*qb9 + qa10*qb10 + qa11*qb11 + qa12*qb12 + qa13*qb13 + qa14*qb14 + qa15*qb15
	sumQA += qa8 + qa9 + qa10 + qa11 + qa12 + qa13 + qa14 + qa15
	sumQB += qb8 + qb9 + qb10 + qb11 + qb12 + qb13 + qb14 + qb15

	qa16 := int64(a[16])
	qb16 := int64(b[16])
	qa17 := int64(a[17])
	qb17 := int64(b[17])
	qa18 := int64(a[18])
	qb18 := int64(b[18])
	qa19 := int64(a[19])
	qb19 := int64(b[19])
	qa20 := int64(a[20])
	qb20 := int64(b[20])
	qa21 := int64(a[21])
	qb21 := int64(b[21])
	qa22 := int64(a[22])
	qb22 := int64(b[22])
	qa23 := int64(a[23])
	qb23 := int64(b[23])
	sumQQ += qa16*qb16 + qa17*qb17 + qa18*qb18 + qa19*qb19 + qa20*qb20 + qa21*qb21 + qa22*qb22 + qa23*qb23
	sumQA += qa16 + qa17 + qa18 + qa19 + qa20 + qa21 + qa22 + qa23
	sumQB += qb16 + qb17 + qb18 + qb19 + qb20 + qb21 + qb22 + qb23

	qa24 := int64(a[24])
	qb24 := int64(b[24])
	qa25 := int64(a[25])
	qb25 := int64(b[25])
	qa26 := int64(a[26])
	qb26 := int64(b[26])
	qa27 := int64(a[27])
	qb27 := int64(b[27])
	qa28 := int64(a[28])
	qb28 := int64(b[28])
	qa29 := int64(a[29])
	qb29 := int64(b[29])
	qa30 := int64(a[30])
	qb30 := int64(b[30])
	qa31 := int64(a[31])
	qb31 := int64(b[31])
	sumQQ += qa24*qb24 + qa25*qb25 + qa26*qb26 + qa27*qb27 + qa28*qb28 + qa29*qb29 + qa30*qb30 + qa31*qb31
	sumQA += qa24 + qa25 + qa26 + qa27 + qa28 + qa29 + qa30 + qa31
	sumQB += qb24 + qb25 + qb26 + qb27 + qb28 + qb29 + qb30 + qb31

	return
}

// dotProdBlock4 computes the block-wise dot product for 4-bit quantized vectors.
// Block size is 32 elements (16 packed bytes).
func dotProdBlock4(a, b []byte, blockLen int) (sumQQ, sumQA, sumQB int64) {
	// 4-bit: 32 elements = 16 packed bytes
	// Bounds check elimination
	_ = a[15]
	_ = b[15]

	for i := 0; i < blockLen; i += 2 {
		byteA := a[i/2]
		byteB := b[i/2]
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
	return
}
