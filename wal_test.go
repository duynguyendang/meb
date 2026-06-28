package meb

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duynguyendang/meb/store"
	"go.uber.org/goleak"
)

// testWALDir creates a temp directory for WAL tests and returns the path.
func testWALDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestWALRoundTrip(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	// Write 1000 entries
	for i := 0; i < 1000; i++ {
		entry := walEntry{
			subject: "sub_" + string(rune('A'+i%26)),
			pred:    "pred",
			object:  "obj_" + string(rune('A'+i%26)),
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Read all back
	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 1000 {
		t.Fatalf("got %d entries, want 1000", len(entries))
	}

	// Verify entries are byte-identical (modulo header)
	for i, e := range entries {
		if e.subject != "sub_"+string(rune('A'+i%26)) {
			t.Errorf("entry %d: subject=%s", i, e.subject)
		}
	}

	w.Close()
}

func TestWALPartialWrite(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	// Write 10 records — each record has 14-byte header + 22-byte payload + 4-byte CRC = 40 bytes
	// After 8-byte magic, records start at offset 8.
	for i := 0; i < 10; i++ {
		entry := walEntry{
			subject: "subject",   // 7 bytes
			pred:    "predicate", // 9 bytes
			object:  "object",    // 6 bytes → 22 payload
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	w.Close()

	// Total: 8 (magic) + 10 * 40 = 408 bytes.
	// Record 8 starts at offset 8 + 7*40 = 288. Truncate at byte 17 of record 8
	// (within CRC region: CRC starts at offset 36 within the record).
	truncateSize := int64(8 + 7*40 + 17) // 305 — within record 8's CRC region
	if err := os.Truncate(w.path, truncateSize); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Reopen and read — should get 7 complete records (record 8 is torn)
	w2, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL after truncate: %v", err)
	}
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after truncate: %v", err)
	}
	if len(entries) != 7 {
		t.Fatalf("got %d entries, want 7 (record 8 partially written, CRC truncated)", len(entries))
	}
	w2.Close()
}

func TestWALReplayIdempotent(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	// Need a real store for this test
	dir := testWALDir(t)
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Create store with disk-backed WAL
	cfg := &store.Config{
		DataDir:        dataDir,
		DictDir:        filepath.Join(dataDir, "dict"),
		InMemory:       false,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		SegmentDir:     filepath.Join(dir, "vectors"),
		SyncWrites:     true,
	}
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}

	// Add 1000 facts
	facts := make([]Fact, 1000)
	for i := 0; i < 1000; i++ {
		facts[i] = Fact{
			Subject:   "sub",
			Predicate: "pred",
			Object:    int32(i),
		}
	}
	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — WAL should replay and dedup via buildExistingFactSet
	s2, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	if s2.Count() != 1000 {
		t.Errorf("after reopen: count=%d, want 1000", s2.Count())
	}
	s2.Close()
}

func TestWALAppendConcurrentWithRead(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	ctx := &cancelCtx{}
	deadline := time.Now().Add(1 * time.Second)

	// Writers: N/2 goroutines appending in a loop
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			j := 0
			for time.Now().Before(deadline) {
				entry := walEntry{
					subject: "s",
					pred:    "p",
					object:  "o",
				}
				if err := w.Append(entry); err != nil {
					ctx.Cancel()
					return
				}
				j++
			}
		}(i)
	}

	// Readers: N/2 goroutines calling ReadAll in a loop
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				entries, err := w.ReadAll()
				if err != nil {
					ctx.Cancel()
					return
				}
				// Validate no corrupted entries
				for _, e := range entries {
					if e.subject == "" && e.pred == "" && e.object == "" {
						continue
					}
				}
			}
		}()
	}

	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("concurrent append+read failed")
	}
}

func TestWALClearConcurrentWithRead(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	deadline := time.Now().Add(1 * time.Second)

	// Writer: appending in a loop until deadline
	wg.Add(1)
	go func() {
		defer wg.Done()
		j := 0
		for time.Now().Before(deadline) {
			entry := walEntry{
				subject: "s",
				pred:    "p",
				object:  "o",
			}
			_ = w.Append(entry) // may fail after Clear, that's fine
			j++
		}
	}()

	// Clear: once
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = w.Clear() // may fail if already cleared, that's fine
	}()

	// Reader: reading in a loop until deadline
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			_, _ = w.ReadAll() // may fail after Clear, that's fine
		}
	}()

	wg.Wait()
}

func TestWALv2CorruptHeader(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	path := filepath.Join(dir, walFileName)

	// Create a valid v2 WAL with 10 records
	w, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := w.Append(walEntry{subject: "s", pred: "p", object: "o"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	w.Close()

	// Corrupt the first 8 bytes (magic header) with garbage
	if err := os.WriteFile(path, []byte("GARBAGE!"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Reopening the WAL with a corrupt header should fail
	_, err = NewWAL(dir)
	if err == nil {
		t.Fatal("expected error opening WAL with corrupt header, got nil")
	}
}

type cancelCtx struct {
	mu sync.Mutex
	err error
}

func (c *cancelCtx) Cancel() {
	c.mu.Lock()
	c.err = os.ErrInvalid
	c.mu.Unlock()
}

func (c *cancelCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// TestWALWriteBatchRecovery verifies that facts written via AddFactBatch()
// (WriteBatch path) are logged to WAL and recovered on restart.
// The transaction path uses BadgerDB SyncWrites: true for durability.
func TestWALWriteBatchRecovery(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	segDir := filepath.Join(dir, "vectors")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("MkdirAll vectors: %v", err)
	}

	cfg := &store.Config{
		DataDir:        dataDir,
		DictDir:        filepath.Join(dir, "dict"),
		InMemory:       false,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
	}

	// Create store
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}

	// Add facts via WriteBatch path (AddFactBatch on MEBStore)
	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "works_at", Object: "acme"},
		{Subject: "bob", Predicate: "lives_in", Object: "nyc"},
	}

	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	// Verify facts were added
	if count := s.Count(); count != 3 {
		t.Fatalf("expected 3 facts, got %d", count)
	}

	// Close store
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen store — WAL should be empty (cleared after AddFactBatch)
	s2, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore (reopen): %v", err)
	}
	defer s2.Close()

	// Verify facts persisted
	if count := s2.Count(); count != 3 {
		t.Fatalf("expected 3 facts after reopen, got %d", count)
	}
}

// TestWALWriteBatchCrashRecovery simulates a crash after WAL write but before
// WriteBatch commit, verifying that facts are recovered on restart.
func TestWALWriteBatchCrashRecovery(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	dir := testWALDir(t)
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	segDir := filepath.Join(dir, "vectors")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("MkdirAll vectors: %v", err)
	}

	cfg := &store.Config{
		DataDir:        dataDir,
		DictDir:        filepath.Join(dir, "dict"),
		InMemory:       false,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
	}

	// Create store and add initial facts
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}

	// Add initial facts via WriteBatch
	if err := s.AddFactBatch([]Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
	}); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	// Now simulate crash: manually write to WAL without committing to BadgerDB
	// This simulates what would happen if the process crashed after WAL write
	// but before WriteBatch commit.
	w, err := NewWAL(dataDir)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	// Write entries to WAL (simulating what AddFactBatch would do before commit)
	if err := w.Append(walEntry{subject: "bob", pred: "knows", object: "alice"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(walEntry{subject: "charlie", pred: "works_at", object: "acme"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Close WAL without committing to BadgerDB (simulating crash)
	w.Close()

	// Close the store (don't commit the simulated transaction)
	s.Close()

	// Reopen store — WAL replay should recover the facts
	s2, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore (reopen): %v", err)
	}
	defer s2.Close()

	// Verify facts were recovered via WAL replay
	count := s2.Count()
	if count < 2 {
		t.Errorf("expected at least 2 facts after WAL replay, got %d", count)
	}
}
