package meb

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
	"github.com/duynguyendang/meb/store"
)

func TestGetTriples(t *testing.T) {
	s := newTestStore(t)

	facts := []Fact{
		NewFact("alice", "knows", "bob"),
		NewFact("alice", "age", int32(42)),
		NewFact("bob", "name", "Bob Smith"),
	}
	for _, f := range facts {
		if err := s.AddFact(f); err != nil {
			t.Fatalf("AddFact(%v): %v", f, err)
		}
	}

	results, err := s.GetTriples([]TripleKey{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "age", Object: int32(42)},
		{Subject: "bob", Predicate: "name", Object: "Bob Smith"},
		{Subject: "alice", Predicate: "knows", Object: "carol"}, // not present
		{Subject: "carol", Predicate: "knows", Object: "bob"},   // unknown subject
	})
	if err != nil {
		t.Fatalf("GetTriples: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	check := func(i int, wantFound bool, wantSubj, wantPred string, wantObj any) {
		r := results[i]
		if r.Found != wantFound {
			t.Errorf("result %d: Found=%v, want %v", i, r.Found, wantFound)
		}
		if !wantFound {
			return
		}
		if r.Fact.Subject != wantSubj || r.Fact.Predicate != wantPred {
			t.Errorf("result %d: got <%s,%s>, want <%s,%s>", i, r.Fact.Subject, r.Fact.Predicate, wantSubj, wantPred)
		}
		if r.Fact.Object != wantObj {
			t.Errorf("result %d: object = %v (%T), want %v (%T)", i, r.Fact.Object, r.Fact.Object, wantObj, wantObj)
		}
	}

	check(0, true, "alice", "knows", "bob")
	check(1, true, "alice", "age", int32(42))
	check(2, true, "bob", "name", "Bob Smith")
	check(3, false, "", "", nil)
	check(4, false, "", "", nil)
}

func TestGetVectors(t *testing.T) {
	s := newTestStore(t)
	dim := s.Vectors().FullDim()

	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	err := s.Update(func(tx *StoreTxn) error {
		return tx.AddVector(7, vec)
	})
	if err != nil {
		t.Fatalf("AddVector: %v", err)
	}

	results, err := s.GetVectors([]uint64{7, 99})
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if !results[0].Found {
		t.Fatalf("vector 7 should be found")
	}
	if len(results[0].Vec) != dim {
		t.Fatalf("expected vector of dim %d, got %d", dim, len(results[0].Vec))
	}
	if results[1].Found {
		t.Fatalf("vector 99 should not be found")
	}
}

func TestGetVectorsMalformedShortValue(t *testing.T) {
	s := newTestStore(t)

	// Write a 1-byte value (hash byte only, no hybrid payload) directly.
	err := s.Update(func(tx *StoreTxn) error {
		return tx.BadgerTxn().Set(keys.EncodeVectorFullKey(5), []byte{0xFF})
	})
	if err != nil {
		t.Fatalf("set malformed vector: %v", err)
	}

	// Must not panic; a short value is treated as missing.
	results, err := s.GetVectors([]uint64{5})
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if results[0].Found {
		t.Fatalf("expected malformed vector to be reported not-found")
	}
}

func TestGetVectorsReconstructionAccuracy(t *testing.T) {
	s := newTestStore(t)
	dim := s.Vectors().FullDim()

	// Deterministic, mostly "interesting" values to exercise the coder.
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(math.Sin(float64(i)*0.1)) / 10
	}
	err := s.Update(func(tx *StoreTxn) error {
		return tx.AddVector(11, vec)
	})
	if err != nil {
		t.Fatalf("AddVector: %v", err)
	}

	results, err := s.GetVectors([]uint64{11})
	if err != nil {
		t.Fatalf("GetVectors: %v", err)
	}
	if !results[0].Found {
		t.Fatalf("vector 11 should be found")
	}
	got := results[0].Vec
	if len(got) != dim {
		t.Fatalf("expected dim %d, got %d", dim, len(got))
	}
	var absErr, maxAbs float64
	for i := range got {
		e := math.Abs(float64(got[i] - vec[i]))
		absErr += e
		if a := math.Abs(float64(vec[i])); a > maxAbs {
			maxAbs = a
		}
	}
	meanRelErr := (absErr / float64(dim)) / (maxAbs + 1e-9)
	// 8-bit blockwise quantization + FWHT reconstruction: a gross offset bug
	// (like GetFullVector's hash-byte mixup) would blow far past 0.1.
	if meanRelErr > 0.1 {
		t.Fatalf("reconstruction too lossy: mean relative error = %.4f", meanRelErr)
	}
}

func TestHealth(t *testing.T) {
	s := newTestStore(t)
	if err := s.AddFact(NewFact("alice", "knows", "bob")); err != nil {
		t.Fatalf("AddFact: %v", err)
	}
	h := s.Health()
	if !h.StoreOpen {
		t.Errorf("expected StoreOpen=true")
	}
	if h.NumFacts != 1 {
		t.Errorf("NumFacts = %d, want 1", h.NumFacts)
	}
	if h.NumVectors != 0 {
		t.Errorf("NumVectors = %d, want 0", h.NumVectors)
	}
	if h.DiskUsage < 0 || h.MemoryInUse == 0 {
		t.Errorf("unexpected health resources: disk=%d mem=%d", h.DiskUsage, h.MemoryInUse)
	}
	// Health() is an alias for DebugInfo(); they must agree.
	d := s.DebugInfo()
	if d != h {
		t.Errorf("Health() and DebugInfo() disagree: %+v vs %+v", h, d)
	}
}

func TestCompact(t *testing.T) {
	dir := t.TempDir()
	cfg := &store.Config{
		DataDir:        dir,
		DictDir:        t.TempDir(),
		InMemory:       false,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		Profile:        "Ingest-Heavy",
		SegmentDir:     filepath.Join(dir, "vectors"),
	}
	if err := os.MkdirAll(cfg.SegmentDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	defer s.Close()

	// Nothing to reclaim on a clean DB -> ErrNoRewrite is expected.
	if err := s.Compact(0.5); err != nil && err != badger.ErrNoRewrite {
		t.Fatalf("Compact: %v", err)
	}
}

func TestBatchWriterCreateFlush(t *testing.T) {
	s := newTestStore(t)

	bw := s.NewBatchWriter()
	facts := []Fact{
		NewFact("alice", "knows", "bob"),
		NewFact("alice", "knows", "carol"),
		NewFact("bob", "name", "Bob"),
	}
	if err := bw.AddFacts(facts); err != nil {
		t.Fatalf("AddFacts: %v", err)
	}
	if got := bw.Count(); got != 3 {
		t.Fatalf("Count() = %d, want 3", got)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := s.Count(); got != 3 {
		t.Fatalf("store.Count() = %d, want 3", got)
	}

	results, err := s.GetTriples([]TripleKey{
		{Subject: "alice", Predicate: "knows", Object: "bob"},
		{Subject: "alice", Predicate: "knows", Object: "carol"},
		{Subject: "bob", Predicate: "name", Object: "Bob"},
	})
	if err != nil {
		t.Fatalf("GetTriples after batch: %v", err)
	}
	for i, r := range results {
		if !r.Found {
			t.Errorf("result %d not found after batch flush", i)
		}
	}
}

func TestBatchWriterSkipsDuplicatesAndExisting(t *testing.T) {
	s := newTestStore(t)

	// Pre-existing fact via the transaction path.
	if err := s.AddFact(NewFact("alice", "knows", "bob")); err != nil {
		t.Fatalf("AddFact: %v", err)
	}

	bw := s.NewBatchWriter()
	facts := []Fact{
		NewFact("alice", "knows", "bob"),   // already in store -> skipped
		NewFact("alice", "knows", "bob"),   // duplicate within this call -> skipped
		NewFact("alice", "knows", "carol"), // new -> counted
		NewFact("bob", "name", "Bob"),      // new -> counted
	}
	if err := bw.AddFacts(facts); err != nil {
		t.Fatalf("AddFacts: %v", err)
	}
	if got := bw.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2 (only carol + Bob)", got)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := s.Count(); got != 3 {
		t.Fatalf("store.Count() = %d, want 3", got)
	}
}

func TestBatchWriterCancelDiscards(t *testing.T) {
	s := newTestStore(t)
	bw := s.NewBatchWriter()
	if err := bw.AddFacts([]Fact{NewFact("alice", "knows", "bob")}); err != nil {
		t.Fatalf("AddFacts: %v", err)
	}
	bw.Cancel()

	if got := s.Count(); got != 0 {
		t.Errorf("count after cancel = %d, want 0", got)
	}
}
