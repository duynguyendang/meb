package vector

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// mmapSegment represents a single memory-mapped file segment.
type mmapSegment struct {
	file       *os.File
	data       []byte
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

	return &mmapSegment{file: f, data: data}, nil
}

func (seg *mmapSegment) close() error {
	var err error
	if seg.data != nil {
		err = syscall.Munmap(seg.data)
		seg.data = nil
	}
	if seg.file != nil {
		if closeErr := seg.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (seg *mmapSegment) sync() error {
	if seg.data == nil || len(seg.data) == 0 {
		return nil
	}
	dataPtr := unsafe.Pointer(&seg.data[0])
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(dataPtr), uintptr(len(seg.data)), uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}
	return nil
}

// cleanupTombstonedSegments unmaps, truncates, and closes all tombstoned segments.
// Called from a background goroutine after the tombstone barrier elapses.
func cleanupTombstonedSegments(segs []*mmapSegment, wg *sync.WaitGroup) {
	defer wg.Done()

	// 5-second barrier for in-flight Search calls to drain their mmap reads
	time.Sleep(5 * time.Second)

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
