package vector

// dotProdBlock8 computes the block-wise dot product for 8-bit quantized vectors.
// Uses int32 accumulators (byte products fit in int32: max 255*255 = 65025)
// and 4 independent accumulator groups to enable CPU pipeline overlap.
// Block size is 32 elements (DefaultHybridBlockSize).
func dotProdBlock8(a, b []byte) (sumQQ, sumQA, sumQB int64) {
	// Bounds check elimination
	_ = a[31]
	_ = b[31]

	// Group 0: elements 0-7 (independent accumulators)
	qa0 := int32(a[0])
	qb0 := int32(b[0])
	qa1 := int32(a[1])
	qb1 := int32(b[1])
	qa2 := int32(a[2])
	qb2 := int32(b[2])
	qa3 := int32(a[3])
	qb3 := int32(b[3])
	qa4 := int32(a[4])
	qb4 := int32(b[4])
	qa5 := int32(a[5])
	qb5 := int32(b[5])
	qa6 := int32(a[6])
	qb6 := int32(b[6])
	qa7 := int32(a[7])
	qb7 := int32(b[7])
	qq0 := qa0*qb0 + qa1*qb1 + qa2*qb2 + qa3*qb3 + qa4*qb4 + qa5*qb5 + qa6*qb6 + qa7*qb7
	qa_s0 := qa0 + qa1 + qa2 + qa3 + qa4 + qa5 + qa6 + qa7
	qb_s0 := qb0 + qb1 + qb2 + qb3 + qb4 + qb5 + qb6 + qb7

	// Group 1: elements 8-15 (independent accumulators)
	qa8 := int32(a[8])
	qb8 := int32(b[8])
	qa9 := int32(a[9])
	qb9 := int32(b[9])
	qa10 := int32(a[10])
	qb10 := int32(b[10])
	qa11 := int32(a[11])
	qb11 := int32(b[11])
	qa12 := int32(a[12])
	qb12 := int32(b[12])
	qa13 := int32(a[13])
	qb13 := int32(b[13])
	qa14 := int32(a[14])
	qb14 := int32(b[14])
	qa15 := int32(a[15])
	qb15 := int32(b[15])
	qq1 := qa8*qb8 + qa9*qb9 + qa10*qb10 + qa11*qb11 + qa12*qb12 + qa13*qb13 + qa14*qb14 + qa15*qb15
	qa_s1 := qa8 + qa9 + qa10 + qa11 + qa12 + qa13 + qa14 + qa15
	qb_s1 := qb8 + qb9 + qb10 + qb11 + qb12 + qb13 + qb14 + qb15

	// Group 2: elements 16-23 (independent accumulators)
	qa16 := int32(a[16])
	qb16 := int32(b[16])
	qa17 := int32(a[17])
	qb17 := int32(b[17])
	qa18 := int32(a[18])
	qb18 := int32(b[18])
	qa19 := int32(a[19])
	qb19 := int32(b[19])
	qa20 := int32(a[20])
	qb20 := int32(b[20])
	qa21 := int32(a[21])
	qb21 := int32(b[21])
	qa22 := int32(a[22])
	qb22 := int32(b[22])
	qa23 := int32(a[23])
	qb23 := int32(b[23])
	qq2 := qa16*qb16 + qa17*qb17 + qa18*qb18 + qa19*qb19 + qa20*qb20 + qa21*qb21 + qa22*qb22 + qa23*qb23
	qa_s2 := qa16 + qa17 + qa18 + qa19 + qa20 + qa21 + qa22 + qa23
	qb_s2 := qb16 + qb17 + qb18 + qb19 + qb20 + qb21 + qb22 + qb23

	// Group 3: elements 24-31 (independent accumulators)
	qa24 := int32(a[24])
	qb24 := int32(b[24])
	qa25 := int32(a[25])
	qb25 := int32(b[25])
	qa26 := int32(a[26])
	qb26 := int32(b[26])
	qa27 := int32(a[27])
	qb27 := int32(b[27])
	qa28 := int32(a[28])
	qb28 := int32(b[28])
	qa29 := int32(a[29])
	qb29 := int32(b[29])
	qa30 := int32(a[30])
	qb30 := int32(b[30])
	qa31 := int32(a[31])
	qb31 := int32(b[31])
	qq3 := qa24*qb24 + qa25*qb25 + qa26*qb26 + qa27*qb27 + qa28*qb28 + qa29*qb29 + qa30*qb30 + qa31*qb31
	qa_s3 := qa24 + qa25 + qa26 + qa27 + qa28 + qa29 + qa30 + qa31
	qb_s3 := qb24 + qb25 + qb26 + qb27 + qb28 + qb29 + qb30 + qb31

	// Combine all 4 groups — each group is independent, CPU pipeline overlaps them
	sumQQ = int64(qq0+qq1+qq2+qq3)
	sumQA = int64(qa_s0+qa_s1+qa_s2+qa_s3)
	sumQB = int64(qb_s0+qb_s1+qb_s2+qb_s3)
	return
}

// dotProdBlock4 computes the block-wise dot product for 4-bit quantized vectors.
// Fully unrolled for 16 packed bytes (32 elements).
func dotProdBlock4(a, b []byte, blockLen int) (sumQQ, sumQA, sumQB int64) {
	// 4-bit: 32 elements = 16 packed bytes
	_ = a[min(15, len(a)-1)]
	_ = b[min(15, len(b)-1)]

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
