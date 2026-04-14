package meb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const walFileName = "meb.wal"

type walEntry struct {
	id      uint64
	subject string
	pred    string
	object  string
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(dataDir string) (*WAL, error) {
	if dataDir == "" {
		return &WAL{}, nil
	}
	path := filepath.Join(dataDir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) Append(entry walEntry) error {
	if w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	subjBytes := []byte(entry.subject)
	predBytes := []byte(entry.pred)
	objBytes := []byte(entry.object)

	buf := make([]byte, 8+2+2+2+len(subjBytes)+len(predBytes)+len(objBytes))
	binary.BigEndian.PutUint64(buf[0:8], entry.id)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(subjBytes)))
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(predBytes)))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(objBytes)))
	copy(buf[14:], subjBytes)
	copy(buf[14+len(subjBytes):], predBytes)
	copy(buf[14+len(subjBytes)+len(predBytes):], objBytes)

	_, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}
	return w.file.Sync()
}

func (w *WAL) ReadAll() ([]walEntry, error) {
	if w.file == nil {
		return nil, nil
	}
	// WAL is append-only — read without holding the mutex.
	// Concurrent Append won't corrupt a read since we snapshot the entire file.
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var entries []walEntry
	offset := 0
	for offset < len(data) {
		if offset+14 > len(data) {
			break
		}
		id := binary.BigEndian.Uint64(data[offset : offset+8])
		subjLen := int(binary.BigEndian.Uint16(data[offset+8 : offset+10]))
		predLen := int(binary.BigEndian.Uint16(data[offset+10 : offset+12]))
		objLen := int(binary.BigEndian.Uint16(data[offset+12 : offset+14]))
		offset += 14

		if offset+subjLen+predLen+objLen > len(data) {
			break
		}

		entry := walEntry{
			id:      id,
			subject: string(data[offset : offset+subjLen]),
			pred:    string(data[offset+subjLen : offset+subjLen+predLen]),
			object:  string(data[offset+subjLen+predLen : offset+subjLen+predLen+objLen]),
		}
		entries = append(entries, entry)
		offset += subjLen + predLen + objLen
	}

	return entries, nil
}

func (w *WAL) Clear() error {
	if w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	return nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
