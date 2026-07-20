package dict

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// newTestEncoder creates an in-memory Encoder for testing.
func newTestEncoder(t *testing.T) *Encoder {
	t.Helper()

	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory BadgerDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	enc, err := NewEncoder(db, 100, 0)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}

	return enc
}

// seedEncoder inserts string→ID pairs into the encoder via GetOrCreateID.
// Returns the IDs in the order the strings were provided.
func seedEncoder(t *testing.T, enc *Encoder, strings ...string) []uint64 {
	t.Helper()
	ids := make([]uint64, len(strings))
	for i, s := range strings {
		id, err := enc.GetOrCreateID(s)
		if err != nil {
			t.Fatalf("GetOrCreateID(%q) failed: %v", s, err)
		}
		ids[i] = id
	}
	return ids
}

func TestGetStrings_Empty(t *testing.T) {
	enc := newTestEncoder(t)

	results, err := enc.GetStrings(nil)
	if err != nil {
		t.Fatalf("GetStrings(nil) returned error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("GetStrings(nil) returned %d results, want 0", len(results))
	}

	results, err = enc.GetStrings([]uint64{})
	if err != nil {
		t.Fatalf("GetStrings(empty) returned error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("GetStrings(empty) returned %d results, want 0", len(results))
	}
}

func TestGetStrings_AllCached(t *testing.T) {
	enc := newTestEncoder(t)
	ids := seedEncoder(t, enc, "alice", "bob", "charlie")

	// First call populates cache
	results, err := enc.GetStrings(ids)
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results[0] != "alice" {
		t.Errorf("results[0] = %q, want %q", results[0], "alice")
	}
	if results[1] != "bob" {
		t.Errorf("results[1] = %q, want %q", results[1], "bob")
	}
	if results[2] != "charlie" {
		t.Errorf("results[2] = %q, want %q", results[2], "charlie")
	}
}

func TestGetStrings_AllCached_PreservesOrder(t *testing.T) {
	enc := newTestEncoder(t)
	ids := seedEncoder(t, enc, "x", "y", "z")

	// All from cache
	results, err := enc.GetStrings(ids)
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results[0] != "x" || results[1] != "y" || results[2] != "z" {
		t.Errorf("order mismatch: got %v, want [x y z]", results)
	}

	// Reverse order
	reversed := []uint64{ids[2], ids[1], ids[0]}
	results, err = enc.GetStrings(reversed)
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if results[0] != "z" || results[1] != "y" || results[2] != "x" {
		t.Errorf("order mismatch: got %v, want [z y x]", results)
	}
}

func TestGetStrings_MixedCache(t *testing.T) {
	enc := newTestEncoder(t)
	ids := seedEncoder(t, enc, "alice", "bob", "charlie")

	// Populate cache with first two IDs only
	_, _ = enc.GetStrings([]uint64{ids[0], ids[1]})

	// Now request all three — first two from cache, third from DB
	results, err := enc.GetStrings(ids)
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results[0] != "alice" || results[1] != "bob" || results[2] != "charlie" {
		t.Errorf("mismatch: got %v, want [alice bob charlie]", results)
	}
}

func TestGetStrings_MissingIDs(t *testing.T) {
	enc := newTestEncoder(t)

	// Request IDs that don't exist
	results, err := enc.GetStrings([]uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for i, r := range results {
		if r != "" {
			t.Errorf("results[%d] = %q, want empty string for missing ID", i, r)
		}
	}
}

func TestGetStrings_MixedMissingAndCached(t *testing.T) {
	enc := newTestEncoder(t)
	ids := seedEncoder(t, enc, "alice")

	// Request mix of existing and non-existing IDs
	query := []uint64{ids[0], 9999, ids[0], 8888}
	results, err := enc.GetStrings(query)
	if err != nil {
		t.Fatalf("GetStrings failed: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
	if results[0] != "alice" {
		t.Errorf("results[0] = %q, want %q", results[0], "alice")
	}
	if results[1] != "" {
		t.Errorf("results[1] = %q, want empty string for missing ID 9999", results[1])
	}
	if results[2] != "alice" {
		t.Errorf("results[2] = %q, want %q (duplicate lookup)", results[2], "alice")
	}
	if results[3] != "" {
		t.Errorf("results[3] = %q, want empty string for missing ID 8888", results[3])
	}
}
