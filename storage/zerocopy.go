package storage

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"unsafe"
)

const (
	PageSize = 4096

	MagicNumber    = 0x4D454230 // "MEB0"
	VersionFormat  = 1
	HeaderSize     = 64
	EntryDirSize   = 16
	AlignedQuadKey = 40 // 33 bytes + 7 padding = 40 (8-byte aligned)
)

type PageHeader struct {
	Magic      uint32
	Version    uint32
	NumEntries uint64
	PageID     uint64
	DataOffset uint32
	Flags      uint32
	Reserved   [32]byte
}

type EntryDir struct {
	KeyOffset  uint32
	KeySize    uint16
	MetaOffset uint32
	MetaSize   uint16
	Padding    uint16
}

type ZeroCopyBuffer struct {
	ptr      unsafe.Pointer
	len      int
	pageID   uint64
	refCount atomic.Int32
}

func NewZeroCopyBuffer(data []byte, pageID uint64) *ZeroCopyBuffer {
	if len(data) == 0 {
		return nil
	}
	return &ZeroCopyBuffer{
		ptr:    unsafe.Pointer(&data[0]),
		len:    len(data),
		pageID: pageID,
	}
}

func (z *ZeroCopyBuffer) Data() []byte {
	if z == nil || z.ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(z.ptr), z.len)
}

func (z *ZeroCopyBuffer) Slice(offset, length int) []byte {
	if z == nil || z.ptr == nil {
		return nil
	}
	if offset < 0 || offset+length > z.len {
		return nil
	}
	if offset%8 != 0 {
		offset = (offset + 7) &^ 7
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(z.ptr)+uintptr(offset))), length)
}

func (z *ZeroCopyBuffer) ReadUint64LE(offset int) uint64 {
	if z == nil || z.ptr == nil {
		return 0
	}
	if offset < 0 || offset+8 > z.len {
		return 0
	}
	if offset%8 != 0 {
		panic("unaligned access: offset must be 8-byte aligned")
	}
	ptr := (*uint64)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
	return *ptr
}

func (z *ZeroCopyBuffer) ReadUint32LE(offset int) uint32 {
	if z == nil || z.ptr == nil {
		return 0
	}
	if offset < 0 || offset+4 > z.len {
		return 0
	}
	if offset%4 != 0 {
		panic("unaligned access: offset must be 4-byte aligned")
	}
	ptr := (*uint32)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
	return *ptr
}

func (z *ZeroCopyBuffer) ReadUint16LE(offset int) uint16 {
	if z == nil || z.ptr == nil {
		return 0
	}
	if offset < 0 || offset+2 > z.len {
		return 0
	}
	ptr := (*uint16)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
	return *ptr
}

func (z *ZeroCopyBuffer) ReadFloat64LE(offset int) float64 {
	bits := z.ReadUint64LE(offset)
	return math.Float64frombits(bits)
}

func (z *ZeroCopyBuffer) ReadFloat32LE(offset int) float32 {
	bits := z.ReadUint32LE(offset)
	return math.Float32frombits(bits)
}

func (z *ZeroCopyBuffer) ReadByte(offset int) byte {
	if z == nil || z.ptr == nil {
		return 0
	}
	if offset < 0 || offset >= z.len {
		return 0
	}
	ptr := (*byte)(unsafe.Pointer(uintptr(z.ptr) + uintptr(offset)))
	return *ptr
}

func (z *ZeroCopyBuffer) DecodeQuadKeyAt(offset int) (prefix byte, s, p, o, g uint64) {
	if z == nil || z.ptr == nil {
		return 0, 0, 0, 0, 0
	}
	if offset%8 != 0 {
		offset = (offset + 7) &^ 7
	}
	prefix = z.ReadByte(offset)
	s = binary.LittleEndian.Uint64(z.Slice(offset+1, 8))
	p = binary.LittleEndian.Uint64(z.Slice(offset+9, 8))
	o = binary.LittleEndian.Uint64(z.Slice(offset+17, 8))
	g = binary.LittleEndian.Uint64(z.Slice(offset+25, 8))
	return
}

func (z *ZeroCopyBuffer) PageID() uint64 {
	if z == nil {
		return 0
	}
	return z.pageID
}

func (z *ZeroCopyBuffer) Len() int {
	if z == nil {
		return 0
	}
	return z.len
}

func (z *ZeroCopyBuffer) Retain() {
	if z == nil {
		return
	}
	z.refCount.Add(1)
}

func (z *ZeroCopyBuffer) Release() {
	if z == nil {
		return
	}
	if z.refCount.Add(-1) == 0 {
		z.ptr = nil
	}
}

func (z *ZeroCopyBuffer) RefCount() int32 {
	if z == nil {
		return 0
	}
	return z.refCount.Load()
}

type PageRef struct {
	buffer *ZeroCopyBuffer
	valid  atomic.Bool
}

func NewPageRef(buffer *ZeroCopyBuffer) *PageRef {
	if buffer == nil {
		return nil
	}
	p := &PageRef{
		buffer: buffer,
	}
	p.valid.Store(true)
	buffer.Retain()
	return p
}

func (p *PageRef) Buffer() *ZeroCopyBuffer {
	if p == nil || !p.valid.Load() {
		return nil
	}
	return p.buffer
}

func (p *PageRef) Slice(offset, length int) []byte {
	if p == nil || !p.valid.Load() {
		return nil
	}
	return p.buffer.Slice(offset, length)
}

func (p *PageRef) ReadUint64LE(offset int) uint64 {
	if p == nil || !p.valid.Load() {
		return 0
	}
	return p.buffer.ReadUint64LE(offset)
}

func (p *PageRef) ReadUint32LE(offset int) uint32 {
	if p == nil || !p.valid.Load() {
		return 0
	}
	return p.buffer.ReadUint32LE(offset)
}

func (p *PageRef) DecodeQuadKeyAt(offset int) (byte, uint64, uint64, uint64, uint64) {
	if p == nil || !p.valid.Load() {
		return 0, 0, 0, 0, 0
	}
	return p.buffer.DecodeQuadKeyAt(offset)
}

func (p *PageRef) Close() {
	if p == nil {
		return
	}
	if p.valid.CompareAndSwap(true, false) && p.buffer != nil {
		p.buffer.Release()
	}
}

func (p *PageRef) IsValid() bool {
	if p == nil {
		return false
	}
	return p.valid.Load()
}
