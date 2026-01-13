package meb

import "fmt"

// Custom error types for better error handling
var (
	ErrInvalidFact   = fmt.Errorf("invalid fact")
	ErrGraphNotFound = fmt.Errorf("graph not found")
	ErrFullTableScan = fmt.Errorf("full table scan not allowed")
	ErrInvalidQuery  = fmt.Errorf("invalid query")
	ErrFactNotFound  = fmt.Errorf("fact not found")
	ErrEmptyBatch    = fmt.Errorf("empty batch")
)
