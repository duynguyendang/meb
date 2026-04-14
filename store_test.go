package meb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/duynguyendang/meb/store"
)

func newTestStore(t *testing.T) *MEBStore {
	t.Helper()
	segDir := filepath.Join(t.TempDir(), "vectors")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("failed to create segment dir: %v", err)
	}
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
	}
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNewMEBStore(t *testing.T) {
	s := newTestStore(t)
	if s.Count() != 0 {
		t.Errorf("expected Count() to be 0, got %d", s.Count())
	}
}

func TestAddFact(t *testing.T) {
	s := newTestStore(t)

	fact := Fact{
		Subject:   "alice",
		Predicate: "knows",
		Object:    "bob",
	}
	err := s.AddFact(fact)
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	if s.Count() != 1 {
		t.Errorf("expected count 1, got %d", s.Count())
	}

	var found bool
	for f, err := range s.Scan("alice", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if f.Subject == "alice" && f.Predicate == "knows" && f.Object == "bob" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find alice-knows-bob fact")
	}
}

func TestAddFactBatch(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "knows", Object: "charlie"},
		{Subject: "bob", Predicate: "knows", Object: "alice"},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	if s.Count() != 3 {
		t.Errorf("expected count 3, got %d", s.Count())
	}
}

func TestAddFactValidation(t *testing.T) {
	s := newTestStore(t)

	tests := []struct {
		name    string
		fact    Fact
		wantErr bool
	}{
		{
			name:    "empty subject",
			fact:    Fact{Subject: "", Predicate: "knows", Object: "bob"},
			wantErr: true,
		},
		{
			name:    "empty predicate",
			fact:    Fact{Subject: "alice", Predicate: "", Object: "bob"},
			wantErr: true,
		},
		{
			name:    "nil object",
			fact:    Fact{Subject: "alice", Predicate: "knows", Object: nil},
			wantErr: true,
		},
		{
			name:    "valid fact",
			fact:    Fact{Subject: "alice", Predicate: "knows", Object: "bob"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.AddFact(tt.fact)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDeleteFactsBySubject(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "knows", Object: "charlie"},
		{Subject: "alice", Predicate: "age", Object: 30},
		{Subject: "bob", Predicate: "knows", Object: "alice"},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	if s.Count() != 4 {
		t.Errorf("expected count 4, got %d", s.Count())
	}

	err = s.DeleteFactsBySubject("alice")
	if err != nil {
		t.Fatalf("DeleteFactsBySubject failed: %v", err)
	}

	if s.Count() != 1 {
		t.Errorf("expected count 1 after delete, got %d", s.Count())
	}

	var aliceCount int
	for _, err := range s.Scan("alice", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		aliceCount++
	}
	if aliceCount != 0 {
		t.Errorf("expected 0 alice facts, got %d", aliceCount)
	}
}

func TestScan(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "knows", Object: "charlie"},
		{Subject: "bob", Predicate: "knows", Object: "alice"},
		{Subject: "bob", Predicate: "age", Object: 25},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	var aliceKnows []Fact
	for f, err := range s.Scan("alice", "knows", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		aliceKnows = append(aliceKnows, f)
	}
	if len(aliceKnows) != 2 {
		t.Errorf("expected 2 alice-knows facts, got %d", len(aliceKnows))
	}

	var knowsBob []Fact
	for f, err := range s.Scan("", "knows", "bob") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		knowsBob = append(knowsBob, f)
	}
	if len(knowsBob) != 1 {
		t.Errorf("expected 1 knows-bob fact, got %d", len(knowsBob))
	}
	if knowsBob[0].Subject != "alice" {
		t.Errorf("expected subject alice, got %s", knowsBob[0].Subject)
	}

	var allFacts []Fact
	for f, err := range s.Scan("", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		allFacts = append(allFacts, f)
	}
	if len(allFacts) != 4 {
		t.Errorf("expected 4 total facts, got %d", len(allFacts))
	}
}

func TestScanEmpty(t *testing.T) {
	s := newTestStore(t)

	// Scan on completely empty store should return 0 results
	var count int
	for _, err := range s.Scan("", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 facts from empty store, got %d", count)
	}
}

func TestExists(t *testing.T) {
	s := newTestStore(t)

	fact := Fact{
		Subject:   "alice",
		Predicate: "knows",
		Object:    "bob",
	}
	err := s.AddFact(fact)
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	if !s.Exists("alice", "knows", "bob") {
		t.Error("expected exists to return true for existing fact")
	}

	if s.Exists("alice", "knows", "charlie") {
		t.Error("expected exists to return false for non-existing fact")
	}

	if s.Exists("bob", "knows", "alice") {
		t.Error("expected exists to return false for non-existing fact")
	}
}

func TestSetContentGetContent(t *testing.T) {
	s := newTestStore(t)

	originalContent := []byte("Hello, World! This is test content for S2 compression.")
	err := s.SetContent(1, originalContent)
	if err != nil {
		t.Fatalf("SetContent failed: %v", err)
	}

	retrieved, err := s.GetContent(1)
	if err != nil {
		t.Fatalf("GetContent failed: %v", err)
	}

	if string(retrieved) != string(originalContent) {
		t.Errorf("content mismatch: got %s, want %s", string(retrieved), string(originalContent))
	}
}

func TestSetContentGetContentNonExistent(t *testing.T) {
	s := newTestStore(t)

	retrieved, err := s.GetContent(999)
	if err != nil {
		t.Fatalf("GetContent failed: %v", err)
	}
	if retrieved != nil {
		t.Errorf("expected nil for non-existent content, got %v", retrieved)
	}
}

func TestAddDocument(t *testing.T) {
	s := newTestStore(t)

	metadata := map[string]any{
		"author": "John Doe",
		"year":   2024,
	}
	content := []byte("Document content here")
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) / 1536.0
	}

	err := s.AddDocument("doc1", content, vec, metadata)
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if s.Count() != 2 {
		t.Errorf("expected count 2 (author and year facts), got %d", s.Count())
	}

	var docFacts []Fact
	for f, err := range s.Scan("doc1", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		docFacts = append(docFacts, f)
	}
	if len(docFacts) != 2 {
		t.Errorf("expected 2 doc facts, got %d", len(docFacts))
	}

	retrievedContent, err := s.GetContentByKey("doc1")
	if err != nil {
		t.Fatalf("GetContentByKey failed: %v", err)
	}
	if string(retrievedContent) != string(content) {
		t.Errorf("content mismatch: got %s, want %s", string(retrievedContent), string(content))
	}
}

func TestDeleteDocument(t *testing.T) {
	s := newTestStore(t)

	metadata := map[string]any{
		"author": "John Doe",
		"year":   2024,
	}
	content := []byte("Document content here")

	err := s.AddDocument("doc1", content, nil, metadata)
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if s.Count() != 2 {
		t.Errorf("expected count 2, got %d", s.Count())
	}

	err = s.DeleteDocument("doc1")
	if err != nil {
		t.Fatalf("DeleteDocument failed: %v", err)
	}

	if s.Count() != 0 {
		t.Errorf("expected count 0 after delete, got %d", s.Count())
	}

	var remaining []Fact
	for f, err := range s.Scan("doc1", "", "") {
		if err != nil {
			// Expected: dictionary entry was deleted, so subject lookup fails
			continue
		}
		remaining = append(remaining, f)
	}
	if len(remaining) != 0 {
		t.Errorf("expected 0 remaining facts, got %d", len(remaining))
	}
}

func TestSetTopicID(t *testing.T) {
	s := newTestStore(t)

	s.SetTopicID(1)
	if s.TopicID() != 1 {
		t.Errorf("expected topicID 1, got %d", s.TopicID())
	}

	s.SetTopicID(100)
	if s.TopicID() != 100 {
		t.Errorf("expected topicID 100, got %d", s.TopicID())
	}

	s.SetTopicID(0xFFFFFF)
	if s.TopicID() != 0xFFFFFF {
		t.Errorf("expected topicID 0xFFFFFF, got %d", s.TopicID())
	}
}

func TestSetTopicIDZero(t *testing.T) {
	s := newTestStore(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on zero topicID")
		}
	}()
	s.SetTopicID(0)
}

func TestRecalculateStats(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "bob", Predicate: "knows", Object: "charlie"},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	count := s.Count()
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	recalculated, err := s.RecalculateStats()
	if err != nil {
		t.Fatalf("RecalculateStats failed: %v", err)
	}

	if recalculated != 2 {
		t.Errorf("expected recalculated 2, got %d", recalculated)
	}

	recalculated, err = s.RecalculateStats()
	if err != nil {
		t.Fatalf("RecalculateStats second call failed: %v", err)
	}
	if recalculated != 2 {
		t.Errorf("expected recalculated 2 on second call, got %d", recalculated)
	}

	err = s.DeleteFactsBySubject("alice")
	if err != nil {
		t.Fatalf("DeleteFactsBySubject failed: %v", err)
	}

	count = s.Count()
	if count != 1 {
		t.Errorf("expected count 1 after delete, got %d", count)
	}

	recalculated, err = s.RecalculateStats()
	if err != nil {
		t.Fatalf("RecalculateStats after delete failed: %v", err)
	}
	if recalculated != 1 {
		t.Errorf("expected recalculated 1, got %d", recalculated)
	}
}

func TestInlineInt32(t *testing.T) {
	s := newTestStore(t)

	err := s.AddFact(Fact{Subject: "counter", Predicate: "value", Object: int32(42)})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	var found bool
	for f, err := range s.Scan("counter", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if v, ok := f.Object.(int32); ok && v == 42 {
			found = true
		}
	}
	if !found {
		t.Error("expected to find counter-value-42 fact with int32 object")
	}
}

func TestInlineFloat32(t *testing.T) {
	s := newTestStore(t)

	err := s.AddFact(Fact{Subject: "pi", Predicate: "value", Object: float32(3.14)})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	var found bool
	for f, err := range s.Scan("pi", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if v, ok := f.Object.(float32); ok {
			diff := v - float32(3.14)
			if diff < 0 {
				diff = -diff
			}
			if diff < 0.01 {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected to find pi-value-3.14 fact with float32 object")
	}
}

func TestInlineBool(t *testing.T) {
	s := newTestStore(t)

	err := s.AddFact(Fact{Subject: "flag", Predicate: "enabled", Object: true})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	var found bool
	for f, err := range s.Scan("flag", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if v, ok := f.Object.(bool); ok && v == true {
			found = true
		}
	}
	if !found {
		t.Error("expected to find flag-enabled-true fact with bool object")
	}
}

func TestInlineNoDictBloat(t *testing.T) {
	s := newTestStore(t)

	// Adding many unique int32 values should NOT create dictionary entries
	for i := int32(0); i < 100; i++ {
		err := s.AddFact(Fact{Subject: "n", Predicate: "val", Object: i})
		if err != nil {
			t.Fatalf("AddFact failed at i=%d: %v", i, err)
		}
	}

	if s.Count() != 100 {
		t.Errorf("expected count 100, got %d", s.Count())
	}

	// Dictionary should only contain "n" and "val", not 100 number strings
	var scanCount int
	for _, err := range s.Scan("n", "val", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		scanCount++
	}
	if scanCount != 100 {
		t.Errorf("expected 100 scan results, got %d", scanCount)
	}
}

func TestInlineMixedTypes(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		{Subject: "doc", Predicate: "title", Object: "My Document"},
		{Subject: "doc", Predicate: "year", Object: int32(2024)},
		{Subject: "doc", Predicate: "published", Object: true},
		{Subject: "doc", Predicate: "rating", Object: float32(4.5)},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	if s.Count() != 4 {
		t.Errorf("expected count 4, got %d", s.Count())
	}

	var docFacts []Fact
	for f, err := range s.Scan("doc", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		docFacts = append(docFacts, f)
	}
	if len(docFacts) != 4 {
		t.Errorf("expected 4 doc facts, got %d", len(docFacts))
	}
}

// --- Transaction tests ---

func TestTransactionAddFactBatch(t *testing.T) {
	s := newTestStore(t)

	err := s.Update(func(txn *StoreTxn) error {
		return txn.AddFactBatch([]Fact{
			{Subject: "alice", Predicate: "knows", Object: "bob"},
			{Subject: "alice", Predicate: "knows", Object: "charlie"},
		})
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if s.Count() != 2 {
		t.Errorf("expected count 2, got %d", s.Count())
	}
}

func TestTransactionRollback(t *testing.T) {
	s := newTestStore(t)

	// Add one fact successfully
	err := s.Update(func(txn *StoreTxn) error {
		return txn.AddFact(Fact{Subject: "alice", Predicate: "knows", Object: "bob"})
	})
	if err != nil {
		t.Fatalf("first Update failed: %v", err)
	}

	if s.Count() != 1 {
		t.Fatalf("expected count 1, got %d", s.Count())
	}

	// Attempt to add facts but rollback
	err = s.Update(func(txn *StoreTxn) error {
		if err := txn.AddFact(Fact{Subject: "charlie", Predicate: "knows", Object: "dave"}); err != nil {
			return err
		}
		return fmt.Errorf("intentional rollback")
	})
	if err == nil {
		t.Fatal("expected error from intentional rollback")
	}

	// Count should still be 1 (charlie fact was rolled back)
	if s.Count() != 1 {
		t.Errorf("expected count 1 after rollback, got %d", s.Count())
	}
}

func TestTransactionView(t *testing.T) {
	s := newTestStore(t)

	s.AddFact(Fact{Subject: "alice", Predicate: "knows", Object: "bob"})

	var facts []Fact
	err := s.View(func(txn *StoreTxn) error {
		for f, err := range txn.Scan("alice", "", "") {
			if err != nil {
				return err
			}
			facts = append(facts, f)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	if len(facts) != 1 {
		t.Errorf("expected 1 fact, got %d", len(facts))
	}
	if facts[0].Subject != "alice" || facts[0].Predicate != "knows" || facts[0].Object != "bob" {
		t.Errorf("unexpected fact: %+v", facts[0])
	}
}

func TestTransactionAddDocument(t *testing.T) {
	s := newTestStore(t)

	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) / 1536.0
	}

	err := s.AddDocument("doc1", []byte("hello world"), vec, map[string]any{
		"author": "alice",
		"year":   int32(2024),
	})
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	// Should have 2 metadata facts (author + year)
	if s.Count() != 2 {
		t.Errorf("expected count 2, got %d", s.Count())
	}

	// Verify content
	content, err := s.GetContentByKey("doc1")
	if err != nil {
		t.Fatalf("GetContentByKey failed: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content mismatch: got %s, want hello world", string(content))
	}

	// Verify metadata
	metadata, err := s.GetDocumentMetadata("doc1")
	if err != nil {
		t.Fatalf("GetDocumentMetadata failed: %v", err)
	}
	if metadata["author"] != "alice" {
		t.Errorf("author mismatch: got %v, want alice", metadata["author"])
	}
}

func TestTransactionDeleteDocument(t *testing.T) {
	s := newTestStore(t)

	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) / 1536.0
	}

	err := s.AddDocument("doc1", []byte("hello world"), vec, map[string]any{
		"author": "alice",
	})
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if s.Count() != 1 {
		t.Fatalf("expected count 1, got %d", s.Count())
	}

	err = s.DeleteDocument("doc1")
	if err != nil {
		t.Fatalf("DeleteDocument failed: %v", err)
	}

	if s.Count() != 0 {
		t.Errorf("expected count 0 after delete, got %d", s.Count())
	}

	// Verify document no longer exists (dictionary entry deleted)
	_, err = s.GetContentByKey("doc1")
	if err == nil {
		t.Errorf("expected error from GetContentByKey after delete, got nil")
	}

	// Verify HasDocument returns false
	exists, err := s.HasDocument("doc1")
	if err != nil {
		t.Fatalf("HasDocument failed: %v", err)
	}
	if exists {
		t.Error("expected HasDocument to return false after delete")
	}
}

func TestTransactionExists(t *testing.T) {
	s := newTestStore(t)

	s.AddFact(Fact{Subject: "alice", Predicate: "knows", Object: "bob"})

	err := s.View(func(txn *StoreTxn) error {
		if !txn.Exists("alice", "knows", "bob") {
			t.Error("expected exists to return true")
		}
		if txn.Exists("alice", "knows", "charlie") {
			t.Error("expected exists to return false")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}
