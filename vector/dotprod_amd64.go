package vector

func dotProdBlock8(a, b []byte) (sumQQ, sumQA, sumQB int64) {
	return dotProdBlock8Generic(a, b)
}

func dotProdBlock4(a, b []byte, blockLen int) (sumQQ, sumQA, sumQB int64) {
	return dotProdBlock4Generic(a, b, blockLen)
}
