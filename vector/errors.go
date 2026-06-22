package vector

import "fmt"

// ErrClosed is returned when an operation is attempted on a VectorRegistry
// that has already been closed.
var ErrClosed = fmt.Errorf("vector registry is closed")
