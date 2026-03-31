package store

import (
	"fmt"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

type Config struct {
	DataDir string

	DictDir string

	InMemory bool

	BlockCacheSize int64

	IndexCacheSize int64

	LRUCacheSize int

	Compression bool

	SyncWrites bool

	NumDictShards int

	MemTableSize int64

	NumMemtables int

	Profile string

	ReadOnly bool

	EnableAutoGC bool

	GCRatio float64

	ValueLogFileSize int64

	Verbose bool

	SegmentDir string
}

func (c *Config) Validate() error {
	if c.InMemory && c.DataDir != "" {
		return fmt.Errorf("DataDir must be empty when InMemory is true")
	}

	if c.DataDir == "" && !c.InMemory {
		return fmt.Errorf("DataDir must be specified when InMemory is false")
	}

	if c.DictDir == "" && !c.InMemory {
		return fmt.Errorf("DictDir must be specified when InMemory is false")
	}

	if c.BlockCacheSize <= 0 {
		return fmt.Errorf("BlockCacheSize must be positive, got %d", c.BlockCacheSize)
	}
	if c.IndexCacheSize <= 0 {
		return fmt.Errorf("IndexCacheSize must be positive, got %d", c.IndexCacheSize)
	}
	if c.LRUCacheSize < 0 {
		return fmt.Errorf("LRUCacheSize must be non-negative, got %d", c.LRUCacheSize)
	}

	if c.MemTableSize > 0 && c.MemTableSize < 1<<20 {
		return fmt.Errorf("MemTableSize must be at least 1MB, got %d", c.MemTableSize)
	}

	if c.NumMemtables < 0 {
		return fmt.Errorf("NumMemtables must be non-negative, got %d", c.NumMemtables)
	}
	if c.NumMemtables > 0 && c.NumMemtables < 2 {
		return fmt.Errorf("NumMemtables must be at least 2, got %d", c.NumMemtables)
	}

	if c.NumDictShards < 0 {
		return fmt.Errorf("NumDictShards must be non-negative, got %d", c.NumDictShards)
	}
	if c.NumDictShards > 0 && (c.NumDictShards&(c.NumDictShards-1)) != 0 {
		return fmt.Errorf("NumDictShards must be 0 or a power of 2, got %d", c.NumDictShards)
	}

	validProfiles := map[string]bool{"Ingest-Heavy": true, "Safe-Serving": true, "ReadOnly": true}
	if c.Profile != "" && !validProfiles[c.Profile] {
		return fmt.Errorf("invalid Profile %q, must be one of: Ingest-Heavy, Safe-Serving, ReadOnly", c.Profile)
	}

	if c.GCRatio < 0 || c.GCRatio > 1 {
		return fmt.Errorf("GCRatio must be between 0 and 1, got %f", c.GCRatio)
	}

	return nil
}

func DefaultConfig(dataDir string) *Config {
	return &Config{
		DataDir:        dataDir,
		DictDir:        filepath.Join(dataDir, "dict"),
		InMemory:       false,
		BlockCacheSize: 8 << 30,
		IndexCacheSize: 2 << 30,
		LRUCacheSize:   100000,
		Compression:    true,
		SyncWrites:     false,
		NumDictShards:  0,
		Profile:        "Ingest-Heavy",
		EnableAutoGC:   true,
		GCRatio:        0.5,
	}
}

func SafeServingConfig(dataDir string) *Config {
	return &Config{
		DataDir:          dataDir,
		DictDir:          filepath.Join(dataDir, "dict"),
		InMemory:         false,
		BlockCacheSize:   0,
		IndexCacheSize:   0,
		LRUCacheSize:     10000,
		Compression:      true,
		SyncWrites:       false,
		NumDictShards:    0,
		Profile:          "Safe-Serving",
		ReadOnly:         true,
		EnableAutoGC:     false,
		GCRatio:          0.5,
		ValueLogFileSize: 64 << 20,
		Verbose:          false,
	}
}

func ReadOnlyConfig(dataDir string) *Config {
	cfg := SafeServingConfig(dataDir)
	cfg.Profile = "ReadOnly"
	cfg.ValueLogFileSize = 16 << 20
	return cfg
}

func buildBadgerOptions(cfg *Config) badger.Options {
	opts := badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger"))

	if cfg.InMemory {
		opts = badger.DefaultOptions("")
		opts.InMemory = true
		return opts
	}

	opts.DetectConflicts = false
	opts.BloomFalsePositive = 0.01

	if cfg.Compression {
		opts.Compression = options.ZSTD
	} else {
		opts.Compression = options.None
	}

	switch cfg.Profile {
	case "Safe-Serving":
		if cfg.ValueLogFileSize > 0 {
			opts.ValueLogFileSize = cfg.ValueLogFileSize
		} else {
			opts.ValueLogFileSize = 64 << 20
		}

		opts.NumCompactors = 2

		if cfg.ReadOnly {
			opts.ReadOnly = true
		}

		opts.NumVersionsToKeep = 1

		if cfg.BlockCacheSize == 0 {
			opts.BlockCacheSize = 0
		}
		if cfg.IndexCacheSize == 0 {
			opts.IndexCacheSize = 0
		}

		if cfg.MemTableSize <= 0 {
			opts.MemTableSize = 8 << 20
		}
		if cfg.NumMemtables <= 0 {
			opts.NumMemtables = 1
		}

	case "ReadOnly":
		opts.ReadOnly = true
		opts.ValueLogFileSize = 16 << 20
		opts.NumCompactors = 2
		opts.NumVersionsToKeep = 1

		if cfg.BlockCacheSize == 0 {
			opts.BlockCacheSize = 0
		}
		if cfg.IndexCacheSize == 0 {
			opts.IndexCacheSize = 0
		}
		opts.MemTableSize = 8 << 20
		opts.NumMemtables = 1

	case "Ingest-Heavy":
		fallthrough
	default:
		if cfg.ValueLogFileSize > 0 {
			opts.ValueLogFileSize = cfg.ValueLogFileSize
		} else {
			opts.ValueLogFileSize = 1 << 30
		}

		opts.NumCompactors = 4
		opts.NumVersionsToKeep = 0
	}

	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.IndexCacheSize = cfg.IndexCacheSize
	opts.SyncWrites = cfg.SyncWrites

	switch cfg.Profile {
	case "Safe-Serving", "ReadOnly":
		if cfg.MemTableSize <= 0 {
			opts.MemTableSize = 16 << 20
		} else {
			opts.MemTableSize = cfg.MemTableSize
		}
		if cfg.NumMemtables <= 0 {
			opts.NumMemtables = 2
		} else {
			opts.NumMemtables = cfg.NumMemtables
		}
	case "Ingest-Heavy", "":
		if cfg.MemTableSize <= 0 {
			opts.MemTableSize = 64 << 20
		} else {
			opts.MemTableSize = cfg.MemTableSize
		}
		if cfg.NumMemtables <= 0 {
			opts.NumMemtables = 3
		} else {
			opts.NumMemtables = cfg.NumMemtables
		}
	}

	return opts
}

func OpenBadgerDB(cfg *Config) (*badger.DB, error) {
	opts := buildBadgerOptions(cfg)
	return badger.Open(opts)
}
