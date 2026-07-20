package dict

type Dictionary interface {
	GetOrCreateID(s string) (uint64, error)

	GetIDs(keys []string) ([]uint64, error)

	GetID(s string) (uint64, error)

	GetString(id uint64) (string, error)

	// GetStrings resolves multiple IDs to strings in a single operation.
	// Missing IDs return empty strings at their position in the result slice.
	// The returned slice has the same length as the input ids.
	GetStrings(ids []uint64) ([]string, error)

	DeleteID(s string) error

	Reset() error

	Close() error
}
