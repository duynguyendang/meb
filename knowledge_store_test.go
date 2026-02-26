package meb

import (
	"testing"

	"github.com/duynguyendang/meb/store"
)

func setupTestStore(t *testing.T) *MEBStore {
	cfg := store.DefaultConfig(t.TempDir())
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAddFactBatchAndScan(t *testing.T) {
	s := setupTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob", Graph: "g1"},
		{Subject: "alice", Predicate: "age", Object: 30, Graph: "g1"},
		{Subject: "bob", Predicate: "knows", Object: "alice", Graph: "g2"},
	}

	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	if s.Count() != 3 {
		t.Errorf("expected 3 facts, got %d", s.Count())
	}

	// Test GSPO
	var g1Facts []Fact
	for f, err := range s.Scan("", "", "", "g1") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		g1Facts = append(g1Facts, f)
	}
	if len(g1Facts) != 2 {
		t.Errorf("expected 2 facts in g1, got %d", len(g1Facts))
	}

	// Test SPO
	var aliceFacts []Fact
	for f, err := range s.Scan("alice", "", "", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		aliceFacts = append(aliceFacts, f)
	}
	if len(aliceFacts) != 2 {
		t.Errorf("expected 2 facts for alice, got %d", len(aliceFacts))
	}
}

func TestDeleteGraph(t *testing.T) {
	s := setupTestStore(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob", Graph: "g1"},
		{Subject: "bob", Predicate: "knows", Object: "alice", Graph: "g2"},
	}
	err := s.AddFactBatch(facts)
	if err != nil {
		t.Fatalf("AddFactBatch failed: %v", err)
	}

	err = s.DeleteGraph("g1")
	if err != nil {
		t.Fatalf("DeleteGraph failed: %v", err)
	}

	// Count isn't proactively decremented unless recalculate or specific deletion logic handles it
	// precisely, but let's see. DeleteGraph removes facts, so Count() should update or we just check the scan.
	// Wait, DeleteGraph reads the GSPO and uses batched deletion, then updates metadata.
	// Let's verify scanning.
	var g1Facts []Fact
	for f, err := range s.Scan("", "", "", "g1") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		g1Facts = append(g1Facts, f)
	}
	if len(g1Facts) != 0 {
		t.Errorf("expected 0 facts in g1, got %d", len(g1Facts))
	}

	// Force stats recalculation just to be sure
	c, _ := s.RecalculateStats()
	if c != 1 {
		t.Errorf("expected 1 fact after delete, got %d", c)
	}
}
