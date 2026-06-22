package meb

import "fmt"

var (
	// ErrInvalidFact is returned when a Fact has missing required fields
	// (empty Subject or Predicate) or an Object of an unsupported type.
	ErrInvalidFact = fmt.Errorf("invalid fact")

	// ErrStoreReadOnly is returned when a write operation is attempted on a
	// store opened with Config.ReadOnly = true.
	ErrStoreReadOnly = fmt.Errorf("store is read-only")

	// ErrInvalidQuery is returned when a query is malformed (e.g. empty
	// predicate in a context that requires one).
	ErrInvalidQuery = fmt.Errorf("invalid query")

	// ErrFactNotFound is returned by lookup helpers when no matching fact
	// is found.
	ErrFactNotFound = fmt.Errorf("fact not found")

	// ErrEmptyBatch is returned when a batch write is called with no facts.
	ErrEmptyBatch = fmt.Errorf("empty batch")

	// ErrWALClosed is returned by WAL.Append or WAL.Clear when the WAL has
	// been closed or is mid-clear. Callers should treat this as a hard
	// failure rather than a silent drop.
	ErrWALClosed = fmt.Errorf("WAL is closed")

	// ErrUnknownTriplePrefix is returned by key encoders when an unknown
	// prefix byte is passed. Surfaces bugs that would otherwise be silently
	// mis-encoded as SPO.
	ErrUnknownTriplePrefix = fmt.Errorf("unknown triple key prefix")
)
