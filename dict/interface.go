package dict

// Dictionary is the interface for dictionary encoders.
// Both Encoder and ShardedEncoder implement this interface.
type Dictionary interface {
	// GetOrCreateID gets the ID for a string, creating a new ID if it doesn't exist.
	GetOrCreateID(s string) (uint64, error)

	// GetIDs gets IDs for multiple strings in a batch, creating new IDs as needed.
	// This is more efficient than calling GetOrCreateID multiple times.
	GetIDs(keys []string) ([]uint64, error)

	// GetID gets the ID for a string without creating a new one.
	GetID(s string) (uint64, error)

	// GetString gets the string for an ID.
	GetString(id uint64) (string, error)

	// Close flushes any pending state and releases resources.
	Close() error
}
