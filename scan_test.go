package meb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/duynguyendang/meb/store"
	"go.uber.org/goleak"
)

func newTestStoreWithConfig(t *testing.T, cfg *store.Config) *MEBStore {
	t.Helper()
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newTestStoreForScan(t *testing.T) *MEBStore {
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
	return newTestStoreWithConfig(t, cfg)
}

func TestScanStringPreserved(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	err := s.AddFact(Fact{Subject: "doc", Predicate: "age", Object: "42"})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("doc", "age", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(string)
		if !ok {
			t.Fatalf("expected string, got %T (%v)", f.Object, f.Object)
		}
		if v != "42" {
			t.Errorf("expected %q, got %q", "42", v)
		}
	}
}

func TestInlineFloat32Unaffected(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

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
	s := newTestStoreWithConfig(t, cfg)

	err := s.AddFact(Fact{Subject: "pi", Predicate: "value", Object: float32(3.14)})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("pi", "value", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(float32)
		if !ok {
			t.Fatalf("expected float32, got %T (%v)", f.Object, f.Object)
		}
		diff := v - float32(3.14)
		if diff < 0 {
			diff = -diff
		}
		if diff >= 0.01 {
			t.Errorf("expected ~3.14, got %f", v)
		}
	}
}

func TestInlineInt32Unaffected(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	err := s.AddFact(Fact{Subject: "num", Predicate: "value", Object: int32(99)})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("num", "value", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(int32)
		if !ok {
			t.Fatalf("expected int32, got %T (%v)", f.Object, f.Object)
		}
		if v != 99 {
			t.Errorf("expected 99, got %d", v)
		}
	}
}

func TestInlineBoolUnaffected(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	err := s.AddFact(Fact{Subject: "flag", Predicate: "value", Object: true})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("flag", "value", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(bool)
		if !ok {
			t.Fatalf("expected bool, got %T (%v)", f.Object, f.Object)
		}
		if !v {
			t.Errorf("expected true, got false")
		}
	}
}

func TestScanPredicateFilter(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	facts := []Fact{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "knows", Object: "charlie"},
		{Subject: "alice", Predicate: "works_at", Object: "acme"},
	}
	for _, f := range facts {
		if err := s.AddFact(f); err != nil {
			t.Fatalf("AddFact: %v", err)
		}
	}

	count := 0
	for f, err := range s.Scan("alice", "knows", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if f.Predicate != "knows" {
			t.Errorf("expected predicate 'knows', got %q", f.Predicate)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 facts, got %d", count)
	}
}

func TestScanContextCancellation(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	for i := 0; i < 100; i++ {
		s.AddFact(Fact{Subject: "sub", Predicate: "pred", Object: fmt.Sprintf("obj_%d", i)})
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	count := 0
	for _, err := range s.ScanContext(ctx, "sub", "pred", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		count++
		if count >= 10 {
			cancel()
			break
		}
	}

	_ = ctx.Err() // may be Canceled
}

func TestScanChunkBoundary(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	// 1100 facts crosses two 512-fact chunks (512 + 512 + 76).
	const n = 1100
	facts := make([]Fact, n)
	for i := 0; i < n; i++ {
		facts[i] = Fact{
			Subject:   fmt.Sprintf("sub_%04d", i),
			Predicate: "rel",
			Object:    fmt.Sprintf("obj_%04d", i),
		}
	}
	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	got, err := Collect(s.Scan("", "rel", ""))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != n {
		t.Fatalf("got %d facts, want %d (chunk boundary handling broken)", len(got), n)
	}

	// Spot-check a few entries across chunk boundaries.
	seen := make(map[string]bool, n)
	for _, f := range got {
		seen[f.Subject] = true
	}
	for _, i := range []int{0, 511, 512, 1023, 1024, 1099} {
		sub := fmt.Sprintf("sub_%04d", i)
		if !seen[sub] {
			t.Errorf("missing fact %s across chunk boundary", sub)
		}
	}
}

func TestScanSubjectsChunkBoundary(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	// 5000 unique subjects crosses the 4096-ID batch boundary.
	const n = 5000
	facts := make([]Fact, n)
	for i := 0; i < n; i++ {
		facts[i] = Fact{
			Subject:   fmt.Sprintf("entity:%05d", i),
			Predicate: "type",
			Object:    "thing",
		}
	}
	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	count := 0
	for range s.ScanSubjects(context.Background()) {
		count++
	}
	if count != n {
		t.Errorf("ScanSubjects returned %d subjects, want %d", count, n)
	}

	count = 0
	for range s.ScanSubjectsByPrefix(context.Background(), "entity:") {
		count++
	}
	// ScanSubjectsByPrefix does not deduplicate — one triple per subject here,
	// so count equals the number of triples (same as subjects in this test).
	if count != n {
		t.Errorf("ScanSubjectsByPrefix returned %d subjects, want %d", count, n)
	}
}
