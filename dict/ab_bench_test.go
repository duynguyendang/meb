package dict

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func benchSetup(b *testing.B, n int) (*Encoder, []uint64) {
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	enc, err := NewEncoder(db, 100, 0) // tiny cache → forces DB path
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id, err := enc.GetOrCreateID(string(rune(i)) + "_subject_string_value")
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}
	return enc, ids
}

func BenchmarkGetStringPerItem(b *testing.B) {
	enc, ids := benchSetup(b, 512)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, id := range ids {
			if _, err := enc.GetString(id); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkGetStringsBatched(b *testing.B) {
	enc, ids := benchSetup(b, 512)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := enc.GetStrings(ids); err != nil {
			b.Fatal(err)
		}
	}
}
