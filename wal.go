package meb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

const (
	walFileName     = "meb.wal"
	walHeaderSize   = 8 + 2 + 2 + 2 + 1 // id:8 + subjLen:2 + predLen:2 + objLen:2 + objType:1
	walCRC32Size    = 4
	walMinRecordLen = walHeaderSize + walCRC32Size // 19 bytes minimum
)

const (
	walObjString uint8 = iota
	walObjBool
	walObjInt32
	walObjFloat32
)

// walMagicV2 is the 8-byte magic header for v2 WAL files: "MEB\0WAL\x02"
// The trailing byte is the format version.
var walMagicV2 = []byte{'M', 'E', 'B', 0, 'W', 'A', 'L', '\x02'}

// crc32cTable is the Castagnoli polynomial (CRC-32C), hardware-accelerated on amd64.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type walEntry struct {
	subject string
	pred    string
	object  string
	objType uint8 // 0=string, 1=bool, 2=int32, 3=float32
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// NewWAL opens or creates a v2 WAL file. Returns an error if an existing WAL
// file has an unsupported format (e.g. v1) — the caller should delete the file.
func NewWAL(dataDir string) (*WAL, error) {
	if dataDir == "" {
		return &WAL{}, nil
	}
	path := filepath.Join(dataDir, walFileName)

	w := &WAL{path: path}

	// Check if WAL file exists
	if _, err := os.Stat(path); err == nil {
		// File exists — validate v2 magic header
		if err := validateWALMagic(path); err != nil {
			return nil, fmt.Errorf("unsupported WAL format: %w; delete the file and retry", err)
		}
	} else if os.IsNotExist(err) {
		// No WAL exists — will create new v2 below
	} else {
		return nil, err
	}

	// Open file for appending (v2 format)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	w.file = f

	// If this is a fresh file, write the magic header
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	if stat.Size() == 0 {
		if _, err := f.Write(walMagicV2); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to write WAL magic header: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to sync WAL magic header: %w", err)
		}
	}

	return w, nil
}

// validateWALMagic reads the first bytes of a WAL file to verify it has the v2 magic header.
func validateWALMagic(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	magic := make([]byte, len(walMagicV2))
	n, err := f.Read(magic)
	if err != nil || n < len(walMagicV2) {
		return fmt.Errorf("WAL file too short for v2 header")
	}

	for i := 0; i < len(walMagicV2); i++ {
		if magic[i] != walMagicV2[i] {
			return fmt.Errorf("invalid WAL magic header")
		}
	}
	return nil
}

// Append writes a single entry to the WAL with CRC32C.
// Returns nil if no WAL is configured (in-memory mode).
// Returns ErrWALClosed if the WAL has been closed or is currently being cleared,
// rather than silently dropping the entry.
func (w *WAL) Append(entry walEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.appendLocked(entry)
}

// AppendBatch writes multiple entries in a single write and single Sync call.
// Returns nil if no WAL is configured (in-memory mode).
// Returns ErrWALClosed if the WAL has been closed or is currently being cleared.
func (w *WAL) AppendBatch(entries []walEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.path == "" {
		return nil
	}
	if w.file == nil {
		return ErrWALClosed
	}

	var buf []byte
	for _, entry := range entries {
		subjBytes := []byte(entry.subject)
		predBytes := []byte(entry.pred)
		objBytes := []byte(entry.object)

		payloadLen := len(subjBytes) + len(predBytes) + len(objBytes)
		entryLen := walHeaderSize + payloadLen + walCRC32Size
		off := len(buf)
		buf = append(buf, make([]byte, entryLen)...)

		binary.BigEndian.PutUint64(buf[off+0:off+8], 0) // reserved
		binary.BigEndian.PutUint16(buf[off+8:off+10], uint16(len(subjBytes)))
		binary.BigEndian.PutUint16(buf[off+10:off+12], uint16(len(predBytes)))
		binary.BigEndian.PutUint16(buf[off+12:off+14], uint16(len(objBytes)))
		buf[off+14] = entry.objType
		dataOff := off + 15
		copy(buf[dataOff:], subjBytes)
		copy(buf[dataOff+len(subjBytes):], predBytes)
		copy(buf[dataOff+len(subjBytes)+len(predBytes):], objBytes)

		crc := crc32.Checksum(buf[off:off+15+payloadLen], crc32cTable)
		copy(buf[len(buf)-walCRC32Size:], crc32Bytes(crc))
	}

	_, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL batch append failed: %w", err)
	}
	return w.file.Sync()
}

func (w *WAL) appendLocked(entry walEntry) error {
	if w.path == "" {
		return nil
	}
	if w.file == nil {
		return ErrWALClosed
	}

	subjBytes := []byte(entry.subject)
	predBytes := []byte(entry.pred)
	objBytes := []byte(entry.object)

	payloadLen := len(subjBytes) + len(predBytes) + len(objBytes)
	buf := make([]byte, walHeaderSize+payloadLen+walCRC32Size)

	binary.BigEndian.PutUint64(buf[0:8], 0) // reserved (was walEntry.id; field removed)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(subjBytes)))
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(predBytes)))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(objBytes)))
	buf[14] = entry.objType
	dataOff := 15
	copy(buf[dataOff:], subjBytes)
	copy(buf[dataOff+len(subjBytes):], predBytes)
	copy(buf[dataOff+len(subjBytes)+len(predBytes):], objBytes)

	// CRC32C covers header + payload (everything except the CRC bytes themselves)
	crc := crc32.Checksum(buf[:dataOff+payloadLen], crc32cTable)
	copy(buf[len(buf)-walCRC32Size:], crc32Bytes(crc))

	_, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	return w.file.Sync()
}

// ReadAll reads all entries from the WAL file (v2 format with magic header and CRC).
// On CRC mismatch (torn last record), returns the valid records so far and logs a warning.
// Must hold mu to be atomic with Clear.
func (w *WAL) ReadAll() ([]walEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.path == "" {
		return nil, nil
	}

	return w.readV2WAL()
}

// readV2WAL reads a v2-format WAL file with magic header and CRC validation.
func (w *WAL) readV2WAL() ([]walEntry, error) {
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(data) < len(walMagicV2) {
		// File too short — no records
		return nil, nil
	}

	offset := len(walMagicV2)
	var entries []walEntry
	var lastGoodCRC bool
	lastGoodCRC = true

	for offset+walMinRecordLen <= len(data) {
		subjLen := int(binary.BigEndian.Uint16(data[offset+8 : offset+10]))
		predLen := int(binary.BigEndian.Uint16(data[offset+10 : offset+12]))
		objLen := int(binary.BigEndian.Uint16(data[offset+12 : offset+14]))
		objType := data[offset+14]
		payloadLen := subjLen + predLen + objLen
		recordLen := walHeaderSize + payloadLen + walCRC32Size

		// Reject records with per-field lengths over the sanity cap (1 MiB)
		// to prevent huge allocations on corrupted WAL data.
		const maxEntryLen = 1 << 20
		if subjLen > maxEntryLen || predLen > maxEntryLen || objLen > maxEntryLen {
			slog.Warn("WAL v2 record has implausible length; stopping read", "offset", offset)
			lastGoodCRC = false
			break
		}

		if offset+recordLen > len(data) {
			// Partial last record — expected after crash
			lastGoodCRC = false
			break
		}

		// Validate CRC
		expectedCRC := binary.BigEndian.Uint32(data[offset+walHeaderSize+payloadLen : offset+recordLen])
		computedCRC := crc32.Checksum(data[offset:offset+walHeaderSize+payloadLen], crc32cTable)

		if computedCRC != expectedCRC {
			// Torn record or corruption
			lastGoodCRC = false
			slog.Warn("WAL CRC mismatch at offset", "offset", offset, "expectedCRC", expectedCRC, "computedCRC", computedCRC)
			break
		}

		dataOff := offset + 15
		entry := walEntry{
			subject: string(data[dataOff : dataOff+subjLen]),
			pred:    string(data[dataOff+subjLen : dataOff+subjLen+predLen]),
			object:  string(data[dataOff+subjLen+predLen : dataOff+subjLen+predLen+objLen]),
			objType: objType,
		}
		entries = append(entries, entry)
		offset += recordLen
	}

	if !lastGoodCRC && len(entries) > 0 {
		slog.Warn("WAL has torn last record (expected after crash); recovered records before it",
			"validRecords", len(entries),
		)
	}

	return entries, nil
}

// Clear atomically clears the WAL file. Uses atomic rename to avoid
// the race between ReadAll (os.ReadFile) and Clear (close+delete+reopen).
func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.path == "" {
		return nil
	}
	if w.file == nil {
		return ErrWALClosed
	}

	// Close current file
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	// Atomic rename: move old file aside, then delete
	deletedPath := w.path + ".deleted"
	if err := os.Rename(w.path, deletedPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	os.Remove(deletedPath) // best-effort; ignore error

	// Create new empty v2 file
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// Write magic header
	if _, err := f.Write(walMagicV2); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	w.file = f
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

// crc32Bytes converts a uint32 CRC to a 4-byte big-endian slice.
func crc32Bytes(crc uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, crc)
	return b
}
