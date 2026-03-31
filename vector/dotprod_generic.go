package vector

func dotProdBlock8Generic(a, b []byte) (sumQQ, sumQA, sumQB int64) {
	for i := 0; i < len(a); i++ {
		qa := int64(a[i])
		qb := int64(b[i])
		sumQQ += qa * qb
		sumQA += qa
		sumQB += qb
	}
	return
}

func dotProdBlock4Generic(a, b []byte, blockLen int) (sumQQ, sumQA, sumQB int64) {
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
