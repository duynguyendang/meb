package store

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestComputeMemoryBudget_GOMEMLIMIT(t *testing.T) {
	twoGiB := int64(2) << 30
	prev := debug.SetMemoryLimit(twoGiB)
	defer debug.SetMemoryLimit(prev)

	detected := DetectMemoryLimit()
	if detected != twoGiB {
		t.Fatalf("DetectMemoryLimit() = %d, want %d", detected, twoGiB)
	}

	maxMmap, dictLRU := ComputeMemoryBudget(0, 100_000)
	wantMmap := int64(float64(twoGiB) * DefaultMmapMemoryFraction)
	if maxMmap != wantMmap {
		t.Errorf("maxMmap = %d, want %d", maxMmap, wantMmap)
	}
	if dictLRU <= 0 || dictLRU > maxAdaptiveDictLRUSize {
		t.Errorf("unexpected adaptive dictLRU: %d", dictLRU)
	}
}

func TestComputeMemoryBudget_ExplicitLimit(t *testing.T) {
	maxMmap, dictLRU := ComputeMemoryBudget(512<<20, 100_000)
	if maxMmap < 200<<20 || maxMmap > 260<<20 {
		t.Errorf("expected 40-50%% of 512MB, got %d", maxMmap)
	}
	if dictLRU < minAdaptiveDictLRUSize || dictLRU > 500_000 {
		t.Errorf("unexpected dictLRU for 512MB: %d", dictLRU)
	}

	// Ensure unbounded / tiny limits handle boundaries safely
	tinyMmap, tinyLRU := ComputeMemoryBudget(1024, 100_000)
	if tinyMmap != 1<<20 {
		t.Errorf("expected floor of 1MB mmap, got %d", tinyMmap)
	}
	if tinyLRU != minAdaptiveDictLRUSize {
		t.Errorf("expected minAdaptiveDictLRUSize=%d, got %d", minAdaptiveDictLRUSize, tinyLRU)
	}

	// Ignore math.MaxInt64 sentinel when explicitly disabled
	prev := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(prev)
	unboundedMmap, unchangedLRU := ComputeMemoryBudget(0, 77_777)
	if DetectMemoryLimit() == 0 {
		if unboundedMmap != 0 || unchangedLRU != 77_777 {
			t.Errorf("expected (0, 77777) when unbounded, got (%d, %d)", unboundedMmap, unchangedLRU)
		}
	}
}
