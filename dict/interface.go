package dict

type Dictionary interface {
	GetOrCreateID(s string) (uint64, error)

	GetIDs(keys []string) ([]uint64, error)

	GetID(s string) (uint64, error)

	GetString(id uint64) (string, error)

	DeleteID(s string) error

	Reset() error

	Close() error
}
