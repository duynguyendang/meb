package vector

import (
	"sync"
	"testing"
)

// TestADCAccumFirstCallRace hammers adcAccum from many goroutines as the very
// first ADC usage in the process, so the lazy dispatch's Once.Do (which may
// reassign the global adcAccum) runs concurrently with call-site reads of that
// same variable. Run in isolation under -race:
//
//	go test ./vector/ -run TestADCAccumFirstCallRace -race
func TestADCAccumFirstCallRace(t *testing.T) {
	const (
		numSubSpaces = 32
		goroutines   = 64
		callsEach    = 2000
	)
	lut := make([]float32, numSubSpaces*256)
	for i := range lut {
		lut[i] = float32(i%256) * 0.01
	}
	codes := make([]byte, numSubSpaces)
	for i := range codes {
		codes[i] = byte(i * 7)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all at once to maximize the first-call window
			var sink float32
			for i := 0; i < callsEach; i++ {
				sink += adcAccum(lut, codes)
			}
			_ = sink
		}()
	}
	close(start)
	wg.Wait()
}
