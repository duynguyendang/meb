package dict

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestDB creates a temporary BadgerDB for testing.
func setupTestDB(t *testing.T) *badger.DB {
	dir := t.TempDir()
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.SyncWrites = false

	db, err := badger.Open(opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return db
}

// setupTestEncoder creates a test encoder with a small cache.
func setupTestEncoder(t *testing.T) *Encoder {
	db := setupTestDB(t)
	enc, err := NewEncoder(db, 100) // Small cache for testing
	require.NoError(t, err)
	return enc
}

func TestDictionaryRoundTrip(t *testing.T) {
	enc := setupTestEncoder(t)

	testCases := []struct {
		name string
		s    string
	}{
		{"simple string", "simple_string"},
		{"with underscores", "string_with_underscores"},
		{"with dashes", "string-with-dashes"},
		{"with dots", "string.with.dots"},
		{"CamelCase", "CamelCaseString"},
		{"with numbers", "string123numbers"},
		{"special chars", "string@#$%"},
		{"unicode", "string_中文_日本語"},
		{"empty string", ""},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			// Get or create ID
			id, err := enc.GetOrCreateID(tt.s)
			require.NoError(t, err)
			require.NotZero(t, id, "ID should not be zero")

			// Verify reverse lookup
			s2, err := enc.GetString(id)
			require.NoError(t, err)
			require.Equal(t, tt.s, s2)

			// Get ID again (should return same ID)
			id2, err := enc.GetOrCreateID(tt.s)
			require.NoError(t, err)
			require.Equal(t, id, id2, "Should return same ID for same string")
		})
	}
}

func TestDictionaryGetIDNotExists(t *testing.T) {
	enc := setupTestEncoder(t)

	// Try to get ID for non-existent string
	_, err := enc.GetID("nonexistent")
	require.ErrorIs(t, err, ErrNotFound)

	// Try to get string for non-existent ID
	_, err = enc.GetString(999)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDictionarySequentialIDs(t *testing.T) {
	enc := setupTestEncoder(t)

	// Create multiple strings and verify sequential IDs
	strings := []string{"a", "b", "c", "d", "e"}
	var ids []uint64

	for _, s := range strings {
		id, err := enc.GetOrCreateID(s)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	// Verify IDs are sequential
	for i := 1; i < len(ids); i++ {
		require.Equal(t, ids[i-1]+1, ids[i], "IDs should be sequential")
	}
}

func TestDictionaryPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "badger")

	// Phase 1: Create some entries
	opts := badger.DefaultOptions(dbPath)
	opts.SyncWrites = false
	db, err := badger.Open(opts)
	require.NoError(t, err)

	enc1, err := NewEncoder(db, 100)
	require.NoError(t, err)

	id1, err := enc1.GetOrCreateID("test_string")
	require.NoError(t, err)
	require.NoError(t, enc1.Close())
	require.NoError(t, db.Close())

	// Phase 2: Reopen and verify
	db2, err := badger.Open(opts)
	require.NoError(t, err)

	enc2, err := NewEncoder(db2, 100)
	require.NoError(t, err)

	// Verify the string still exists
	s, err := enc2.GetString(id1)
	require.NoError(t, err)
	require.Equal(t, "test_string", s)

	// Verify GetOrCreateID returns the same ID
	sameID, err := enc2.GetOrCreateID("test_string")
	require.NoError(t, err)
	require.Equal(t, id1, sameID)

	// Verify new strings get new IDs (sequential)
	id3, err := enc2.GetOrCreateID("new_string")
	require.NoError(t, err)
	require.Greater(t, id3, id1, "New ID should be greater than old ID")

	require.NoError(t, enc2.Close())
	require.NoError(t, db2.Close())
}

func TestDictionaryConcurrency(t *testing.T) {
	enc := setupTestEncoder(t)

	const numGoroutines = 100
	const stringsPerGoroutine = 1000

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*stringsPerGoroutine)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < stringsPerGoroutine; j++ {
				s := fmt.Sprintf("string_%d_%d", n, j)
				id, err := enc.GetOrCreateID(s)
				if err != nil {
					errors <- err
					return
				}

				// Verify reverse lookup
				s2, err := enc.GetString(id)
				if err != nil {
					errors <- err
					return
				}
				if s != s2 {
					errors <- fmt.Errorf("round-trip failed: %s != %s", s, s2)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		require.NoError(t, err)
	}

	// Verify final state
	stats := enc.Stats()
	assert.Equal(t, uint64(numGoroutines*stringsPerGoroutine+1), stats["next_id"])
}

func TestDictionaryCacheEffectiveness(t *testing.T) {
	enc := setupTestEncoder(t)

	// Create an entry
	id, err := enc.GetOrCreateID("cached_string")
	require.NoError(t, err)

	// First lookup should hit DB, second should hit cache
	_, err = enc.GetString(id)
	require.NoError(t, err)

	statsBefore := enc.Stats()
	cacheLenBefore := statsBefore["reverse_cache_len"]

	// Lookup again (should be in cache)
	_, err = enc.GetString(id)
	require.NoError(t, err)

	statsAfter := enc.Stats()
	cacheLenAfter := statsAfter["reverse_cache_len"]

	// Cache length should be the same (cache hit, not miss)
	assert.Equal(t, cacheLenBefore, cacheLenAfter)
}

func TestDictionaryLRUEviction(t *testing.T) {
	// Create encoder with very small cache
	db := setupTestDB(t)
	enc, err := NewEncoder(db, 2) // Only 2 items in cache
	require.NoError(t, err)

	// Add 3 items
	id1, _ := enc.GetOrCreateID("string1")
	_, _ = enc.GetOrCreateID("string2")
	id3, _ := enc.GetOrCreateID("string3")

	// Access string1 and string3 to populate cache
	enc.GetString(id1)
	enc.GetString(id3)

	// Cache should now have: [string1, string3]
	// string2 should have been evicted

	// Verify stats
	stats := enc.Stats()
	// Cache length should be at most 2 (LRU eviction)
	assert.LessOrEqual(t, stats["reverse_cache_len"], 2)
}

func TestDictionaryBatchOperations(t *testing.T) {
	enc := setupTestEncoder(t)

	// Create many strings efficiently
	const count = 10000
	var ids []uint64

	for i := 0; i < count; i++ {
		s := fmt.Sprintf("batch_string_%d", i)
		id, err := enc.GetOrCreateID(s)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	// Verify all IDs are unique
	idMap := make(map[uint64]bool)
	for _, id := range ids {
		require.False(t, idMap[id], "Duplicate ID found: %d", id)
		idMap[id] = true
	}

	// Verify count
	require.Len(t, idMap, count)
}

func TestDictionarySpecialCharacters(t *testing.T) {
	enc := setupTestEncoder(t)

	specialStrings := []string{
		"string\nwith\nnewlines",
		"string\twith\ttabs",
		"string\x00null",
		"string\"with\"quotes",
		"string'with'apostrophes",
		"string\\with\\backslashes",
		"string/with/slashes",
		"string🧪with🚀emojis",
	}

	for _, s := range specialStrings {
		t.Run(fmt.Sprintf("special_%q", s), func(t *testing.T) {
			id, err := enc.GetOrCreateID(s)
			require.NoError(t, err)

			s2, err := enc.GetString(id)
			require.NoError(t, err)
			require.Equal(t, s, s2)
		})
	}
}

func TestDictionaryLargeStrings(t *testing.T) {
	enc := setupTestEncoder(t)

	// Create a very large string
	largeString := string(make([]byte, 10000)) // 10KB string
	for i := range largeString {
		largeString = largeString[:i] + "A" + largeString[i+1:]
	}

	id, err := enc.GetOrCreateID(largeString)
	require.NoError(t, err)

	s2, err := enc.GetString(id)
	require.NoError(t, err)
	require.Equal(t, largeString, s2)
}

func TestDictionaryStats(t *testing.T) {
	enc := setupTestEncoder(t)

	// Initial stats
	stats := enc.Stats()
	assert.Equal(t, uint64(1), stats["next_id"]) // Starts at 1
	assert.Equal(t, 0, stats["forward_cache_len"])
	assert.Equal(t, 0, stats["reverse_cache_len"])

	// Add some entries
	enc.GetOrCreateID("test1")
	enc.GetOrCreateID("test2")

	stats = enc.Stats()
	assert.Equal(t, uint64(3), stats["next_id"]) // 1 + 2 entries
	assert.Greater(t, stats["forward_cache_len"], 0)
	assert.Greater(t, stats["reverse_cache_len"], 0)
}

// Benchmark tests

func BenchmarkDictionaryGetOrCreateID(b *testing.B) {
	dir := b.TempDir()
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.SyncWrites = false
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	enc, err := NewEncoder(db, 10000)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("bench_string_%d", i%1000)
		_, _ = enc.GetOrCreateID(s)
	}
}

func BenchmarkDictionaryGetString(b *testing.B) {
	dir := b.TempDir()
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.SyncWrites = false
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	enc, err := NewEncoder(db, 10000)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-populate
	ids := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		s := fmt.Sprintf("bench_string_%d", i)
		ids[i], _ = enc.GetOrCreateID(s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.GetString(ids[i%1000])
	}
}

func BenchmarkDictionaryConcurrent(b *testing.B) {
	dir := b.TempDir()
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.SyncWrites = false
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	enc, err := NewEncoder(db, 100000)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s := fmt.Sprintf("concurrent_%d", i%10000)
			_, _ = enc.GetOrCreateID(s)
			i++
		}
	})
}
