package meb

import "fmt"

var (
	ErrInvalidFact   = fmt.Errorf("invalid fact")
	ErrStoreReadOnly = fmt.Errorf("store is read-only")
	ErrInvalidQuery  = fmt.Errorf("invalid query")
	ErrFactNotFound  = fmt.Errorf("fact not found")
	ErrEmptyBatch    = fmt.Errorf("empty batch")
)
