//go:build arm64

package vector

import "golang.org/x/sys/cpu"

func init() {
	if cpu.ARM64.HasASIMD {
		adcAccum = adcAccumNEON
		return
	}
	adcAccum = adcAccumScalar
}
