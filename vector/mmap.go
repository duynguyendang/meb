package vector

import (
	"fmt"
	"os"
	"syscall"
)

type mmapFile struct {
	file *os.File
	data []byte
}

func newMmapFile(path string, size int) (*mmapFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open mmap file: %w", err)
	}

	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to truncate mmap file: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &mmapFile{file: f, data: data}, nil
}

func (mf *mmapFile) close() error {
	if mf.data != nil {
		syscall.Munmap(mf.data)
		mf.data = nil
	}
	if mf.file != nil {
		return mf.file.Close()
	}
	return nil
}

func (mf *mmapFile) grow(newSize int) error {
	if mf.data != nil {
		syscall.Munmap(mf.data)
	}

	if err := mf.file.Truncate(int64(newSize)); err != nil {
		return fmt.Errorf("failed to grow mmap file: %w", err)
	}

	data, err := syscall.Mmap(int(mf.file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to re-mmap file: %w", err)
	}

	mf.data = data
	return nil
}

func (mf *mmapFile) sync() error {
	if mf.data == nil {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(mf.data[0]), uintptr(len(mf.data)), uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}
	return nil
}
