//go:build linux

package vector

import (
	"fmt"
	"syscall"
	"unsafe"
)

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
