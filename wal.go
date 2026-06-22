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
	walMagicSize    = 8
	walHeaderSize   = 8 + 2 + 2 + 2 // id:8 + subjLen:2 + predLen:2 + objLen:2
	walCRC32Size    = 4
	walMinRecordLen = walHeaderSize + walCRC32Size // 18 bytes minimum
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
}

// WALVersion describes the on-disk WAL format.
type WALVersion int

const (
	WALVersionUnknown WALVersion = 0
	WALVersion1       WALVersion = 1 // no magic, no CRC
	WALVersion2       WALVersion = 2 // magic header + CRC32C per record
)

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	version WALVersion
}

// NewWAL opens or creates a WAL file. If a v1 WAL exists, it is migrated
// to v2 format on open. The v1 file is only deleted after the v2 file is fsynced.
func NewWAL(dataDir string) (*WAL, error) {
	if dataDir == "" {
		return &WAL{version: WALVersion2}, nil
	}
	path := filepath.Join(dataDir, walFileName)

	w := &WAL{path: path}

	// Check if v1 or v2 WAL exists
	if _, err := os.Stat(path); err == nil {
		// File exists — detect version
		version, err := detectWALVersion(path)
		if err != nil {
			return nil, fmt.Errorf("failed to detect WAL version: %w", err)
		}
		w.version = version

		if version == WALVersion1 {
			// Migrate v1 → v2
			if err := w.migrateV1ToV2(); err != nil {
				return nil, fmt.Errorf("WAL v1→v2 migration failed: %w", err)
			}
		}
	} else if os.IsNotExist(err) {
		// No WAL exists — create new v2
		w.version = WALVersion2
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

// detectWALVersion reads the first bytes of a WAL file to determine its format.
func detectWALVersion(path string) (WALVersion, error) {
	f, err := os.Open(path)
	if err != nil {
		return WALVersionUnknown, err
	}
	defer f.Close()

	magic := make([]byte, len(walMagicV2))
	n, err := f.Read(magic)
	if err != nil || n < len(walMagicV2) {
		// File too short for magic — treat as v1 (or empty)
		return WALVersion1, nil
	}

	// Check for v2 magic
	match := true
	for i := 0; i < len(walMagicV2); i++ {
		if magic[i] != walMagicV2[i] {
			match = false
			break
		}
	}
	if match {
		return WALVersion2, nil
	}

	// No magic header — v1 format
	return WALVersion1, nil
}

// migrateV1ToV2 reads the v1 WAL, rewrites it as v2, fsyncs the v2 file,
// then deletes the v1 file.
func (w *WAL) migrateV1ToV2() error {
	slog.Info("migrating WAL from v1 to v2 format", "path", w.path)

	// Read v1 entries
	v1Data, err := os.ReadFile(w.path)
	if err != nil {
		return fmt.Errorf("failed to read v1 WAL: %w", err)
	}

	entries, err := parseV1WAL(v1Data)
	if err != nil {
		// Leave v1 WAL in place; refuse to open
		return fmt.Errorf("failed to parse v1 WAL (data not lost): %w", err)
	}

	// Write v2 file to a temp path
	tmpPath := w.path + ".v2tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create v2 temp file: %w", err)
	}

	// Write magic header
	if _, err := f.Write(walMagicV2); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write v2 header: %w", err)
	}

	// Write each entry in v2 format
	for _, e := range entries {
		subjBytes := []byte(e.subject)
		predBytes := []byte(e.pred)
		objBytes := []byte(e.object)

		recLen := walHeaderSize + len(subjBytes) + len(predBytes) + len(objBytes) + walCRC32Size
		buf := make([]byte, recLen)
		binary.BigEndian.PutUint64(buf[0:8], 0) // reserved
		binary.BigEndian.PutUint16(buf[8:10], uint16(len(subjBytes)))
		binary.BigEndian.PutUint16(buf[10:12], uint16(len(predBytes)))
		binary.BigEndian.PutUint16(buf[12:14], uint16(len(objBytes)))
		copy(buf[14:], subjBytes)
		copy(buf[14+len(subjBytes):], predBytes)
		copy(buf[14+len(subjBytes)+len(predBytes):], objBytes)

		// CRC32C over header + payload
		crc := crc32.Checksum(buf[:14+len(subjBytes)+len(predBytes)+len(objBytes)], crc32cTable)
		copy(buf[len(buf)-walCRC32Size:], crc32Bytes(crc))

		if _, err := f.Write(buf); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write v2 record: %w", err)
		}
	}

	// fsync the v2 file before replacing the v1 file
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to fsync v2 file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Rename v2 over v1 (atomic on same filesystem)
	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename v2 over v1: %w", err)
	}

	w.version = WALVersion2
	slog.Info("WAL migration complete", "entries", len(entries))
	return nil
}

// parseV1WAL parses a v1 WAL byte slice (no magic, no CRC).
// Validates each record's length fields to avoid huge allocations from
// corrupted length values. A partial last record is treated as a torn write
// (recovered records before it are returned).
func parseV1WAL(data []byte) ([]walEntry, error) {
	const maxEntryLen = 1 << 20 // 1 MiB sanity cap per field

	var entries []walEntry
	offset := 0
	for offset+14 <= len(data) {
		subjLen := int(binary.BigEndian.Uint16(data[offset+8 : offset+10]))
		predLen := int(binary.BigEndian.Uint16(data[offset+10 : offset+12]))
		objLen := int(binary.BigEndian.Uint16(data[offset+12 : offset+14]))
		offset += 14

		// Validate per-field lengths against a sanity cap so a corrupted
		// WAL can't trigger gigabyte allocations.
		if subjLen > maxEntryLen || predLen > maxEntryLen || objLen > maxEntryLen {
			return entries, fmt.Errorf("WAL v1 record at offset %d has implausible length (subj=%d, pred=%d, obj=%d)", offset-14, subjLen, predLen, objLen)
		}

		if offset+subjLen+predLen+objLen > len(data) {
			// Partial last record — stop here (crash during write)
			break
		}

		entry := walEntry{
			subject: string(data[offset : offset+subjLen]),
			pred:    string(data[offset+subjLen : offset+subjLen+predLen]),
			object:  string(data[offset+subjLen+predLen : offset+subjLen+predLen+objLen]),
		}
		entries = append(entries, entry)
		offset += subjLen + predLen + objLen
	}
	return entries, nil
}

// Append writes a single entry to the WAL with CRC32C.
// Returns nil if no WAL is configured (in-memory mode).
// Returns ErrWALClosed if the WAL has been closed or is currently being cleared,
// rather than silently dropping the entry.
func (w *WAL) Append(entry walEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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
	copy(buf[14:], subjBytes)
	copy(buf[14+len(subjBytes):], predBytes)
	copy(buf[14+len(subjBytes)+len(predBytes):], objBytes)

	// CRC32C covers header + payload (everything except the CRC bytes themselves)
	crc := crc32.Checksum(buf[:14+payloadLen], crc32cTable)
	copy(buf[len(buf)-walCRC32Size:], crc32Bytes(crc))

	_, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	return w.file.Sync()
}

// ReadAll reads all entries from the WAL file. It handles both v1 and v2 formats.
// On CRC mismatch (torn last record), returns the valid records so far and logs a warning.
func (w *WAL) ReadAll() ([]walEntry, error) {
	if w.path == "" {
		return nil, nil
	}

	w.mu.Lock()
	version := w.version
	w.mu.Unlock()

	switch version {
	case WALVersion2:
		return w.readV2WAL()
	case WALVersion1:
		return w.readV1WAL()
	default:
		return nil, nil
	}
}

// readV1WAL reads a v1-format WAL file (no magic, no CRC).
func (w *WAL) readV1WAL() ([]walEntry, error) {
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseV1WAL(data)
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

		entry := walEntry{
			subject: string(data[offset+14 : offset+14+subjLen]),
			pred:    string(data[offset+14+subjLen : offset+14+subjLen+predLen]),
			object:  string(data[offset+14+subjLen+predLen : offset+14+subjLen+predLen+objLen]),
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
	w.version = WALVersion2
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
