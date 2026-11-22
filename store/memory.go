package store

import (
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// DefaultMmapMemoryFraction is the fraction of the detected memory ceiling
	// allocated to the mmap vector cache (45%, within the 40-50% target window).
	DefaultMmapMemoryFraction = 0.45

	// DefaultDictMemoryFraction is the fraction of the detected memory ceiling
	// allocated to the dictionary forward/reverse LRU caches (10%).
	DefaultDictMemoryFraction = 0.10

	// estimatedBytesPerDictLRUEntry approximates the heap footprint of one
	// string<->uint64 entry pair across forwardCache and reverseCache.
	estimatedBytesPerDictLRUEntry = 256

	minAdaptiveDictLRUSize = 2_048
	maxAdaptiveDictLRUSize = 500_000
)

// DetectMemoryLimit returns the effective memory ceiling in bytes for the
// current process, or 0 if no limit is configured or detected.
//
// Resolution order:
//  1. Go runtime GOMEMLIMIT via debug.SetMemoryLimit(-1) (if < math.MaxInt64)
//  2. Linux cgroup v2 (/sys/fs/cgroup/memory.max)
//  3. Linux cgroup v1 (/sys/fs/cgroup/memory/memory.limit_in_bytes)
func DetectMemoryLimit() int64 {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 {
		return limit
	}
	if limit := readCgroupMemoryLimit("/sys/fs/cgroup/memory.max"); limit > 0 {
		return limit
	}
	if limit := readCgroupMemoryLimit("/sys/fs/cgroup/memory/memory.limit_in_bytes"); limit > 0 {
		return limit
	}
	return 0
}

func readCgroupMemoryLimit(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil || val <= 0 {
		return 0
	}
	// Ignore sentinel "unlimited" values near MaxInt64 / page-aligned 2^63 (~1 PiB+).
	const maxReasonableCgroupBytes = int64(1) << 50
	if val >= maxReasonableCgroupBytes {
		return 0
	}
	return val
}

// ComputeMemoryBudget derives the maximum mmap vector cache budget (40-50% of
// RAM ceiling) and an adaptive dictionary LRU cache capacity from a memory
// limit in bytes.
//
// If memLimitBytes <= 0, DetectMemoryLimit() is consulted. If still 0,
// (0, currentLRUSize) is returned (unbounded mmap, unchanged dict LRU).
func ComputeMemoryBudget(memLimitBytes int64, currentLRUSize int) (maxMmapBytes int64, dictLRUSize int) {
	if memLimitBytes <= 0 {
		memLimitBytes = DetectMemoryLimit()
	}
	if memLimitBytes <= 0 {
		return 0, currentLRUSize
	}

	maxMmapBytes = int64(float64(memLimitBytes) * DefaultMmapMemoryFraction)
	if maxMmapBytes < 1<<20 {
		maxMmapBytes = 1 << 20 // minimum 1 MiB when bounded
	}

	dictBudgetBytes := int64(float64(memLimitBytes) * DefaultDictMemoryFraction)
	computedLRU := int(dictBudgetBytes / estimatedBytesPerDictLRUEntry)
	if computedLRU < minAdaptiveDictLRUSize {
		computedLRU = minAdaptiveDictLRUSize
	} else if computedLRU > maxAdaptiveDictLRUSize {
		computedLRU = maxAdaptiveDictLRUSize
	}

	return maxMmapBytes, computedLRU
}
