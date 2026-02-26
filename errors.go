package meb

import "fmt"

// Custom error types for better error handling
var (
	ErrInvalidFact   = fmt.Errorf("invalid fact")
	ErrGraphNotFound = fmt.Errorf("graph not found")
	ErrStoreReadOnly = fmt.Errorf("store is read-only")
	ErrInvalidQuery  = fmt.Errorf("invalid query")
	ErrFactNotFound  = fmt.Errorf("fact not found")
	ErrEmptyBatch    = fmt.Errorf("empty batch")
)
