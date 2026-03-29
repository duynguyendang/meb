package vector

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// mmapSegment represents a single memory-mapped file segment.
type mmapSegment struct {
	file *os.File
	data []byte
}

// newMmapSegment creates a new mmap segment file of the given size.
func newMmapSegment(dir string, index int, size int) (*mmapSegment, error) {
	os.MkdirAll(dir, 0755)

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
