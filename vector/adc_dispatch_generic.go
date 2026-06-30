//go:build !amd64 && !arm64

package vector

func init() {
	adcAccum = adcAccumScalar
}
