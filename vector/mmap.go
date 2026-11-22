package vector

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// tombstoneBarrier is the delay before tombstoned mmap segments are unmapped.
// This allows in-flight Search() calls to drain their mmap reads.
const tombstoneBarrier = 5 * time.Second

// mmapSegment represents a single memory-mapped file segment.
type mmapSegment struct {
	index      int
	size       int
	file       *os.File
	data       []byte
	lastAccess atomic.Int64
	evicted    atomic.Bool
	tombstoned bool // if true, marked for deferred cleanup
}

// newMmapSegment creates a new mmap segment file of the given size.
func newMmapSegment(dir string, index int, size int) (*mmapSegment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("vectors.%d.tq", index))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to truncate segment file: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap segment: %w", err)
	}

	return &mmapSegment{index: index, size: size, file: f, data: data}, nil
}

// unmap flushes and releases the OS virtual mapping and file descriptor for LRU eviction.
// Must only be called when no readers are concurrently accessing seg.data (e.g., under r.mu.Lock()).
func (seg *mmapSegment) unmap() error {
	seg.evicted.Store(true)
	return seg.close()
}

func (seg *mmapSegment) close() error {
	var err error
	if seg.data != nil {
		if syncErr := seg.sync(); syncErr != nil {
			err = syncErr
		}
		if unmapErr := syscall.Munmap(seg.data); unmapErr != nil && err == nil {
			err = unmapErr
		}
		seg.data = nil
	}
	if seg.file != nil {
		if closeErr := seg.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		seg.file = nil
	}
	return err
}

// sync is platform-specific — see mmap_linux.go and mmap_other.go.

// cleanupTombstonedSegments unmaps, truncates, and closes all tombstoned segments.
// Called from a background goroutine after the tombstone barrier elapses.
func cleanupTombstonedSegments(segs []*mmapSegment, wg *sync.WaitGroup) {
	defer wg.Done()

	// tombstoneBarrier delay for in-flight Search calls to drain their mmap reads
	time.Sleep(tombstoneBarrier)

	for _, seg := range segs {
		if seg == nil {
			continue
		}
		if seg.data != nil {
			if err := syscall.Munmap(seg.data); err != nil {
				slog.Warn("tombstoned segment munmap failed", "error", err)
			}
			seg.data = nil
		}
		if seg.file != nil {
			if err := seg.file.Truncate(0); err != nil {
				slog.Warn("tombstoned segment truncate failed", "error", err)
			}
			if err := seg.file.Close(); err != nil {
				slog.Warn("tombstoned segment close failed", "error", err)
			}
			seg.file = nil
		}
	}
}
