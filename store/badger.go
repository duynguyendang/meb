package store

import (
	"fmt"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// Config holds the configuration for BadgerDB.
type Config struct {
	// DataDir is the directory where BadgerDB will store its data.
	DataDir string

	// InMemory enables in-memory mode (useful for testing).
	InMemory bool

	// BlockCacheSize is the size of the block cache in bytes.
	// Recommended: 8GB for production with 1B nodes.
	BlockCacheSize int64

	// IndexCacheSize is the size of the index cache in bytes.
	// Recommended: 1GB for production.
	IndexCacheSize int64

	// LRUCacheSize is the size of the dictionary LRU cache.
	// Recommended: 100,000 items.
	LRUCacheSize int

	// Compression enables ZSTD compression.
	// Recommended: enabled for production.
	Compression bool

	// SyncWrites enables synchronous writes.
	// Disabled for performance, but may lose recent writes on crash.
	SyncWrites bool

	// NumDictShards is the number of shards for the dictionary encoder.
	// Use 0 for single-threaded encoder (default), or a power of 2 (e.g., 16) for sharded encoder.
	// Sharded encoder reduces contention in concurrent workloads.
	NumDictShards int
}

// Validate checks if the configuration is valid and returns an error if not.
func (c *Config) Validate() error {
	// Validate DataDir
	if c.DataDir == "" && !c.InMemory {
		return fmt.Errorf("DataDir must be specified when InMemory is false")
	}

	// Validate cache sizes
	if c.BlockCacheSize <= 0 {
		return fmt.Errorf("BlockCacheSize must be positive, got %d", c.BlockCacheSize)
	}
	if c.IndexCacheSize <= 0 {
		return fmt.Errorf("IndexCacheSize must be positive, got %d", c.IndexCacheSize)
	}
	if c.LRUCacheSize < 0 {
		return fmt.Errorf("LRUCacheSize must be non-negative, got %d", c.LRUCacheSize)
	}

	// Validate NumDictShards: must be 0 or a power of 2
	if c.NumDictShards < 0 {
		return fmt.Errorf("NumDictShards must be non-negative, got %d", c.NumDictShards)
	}
	if c.NumDictShards > 0 && (c.NumDictShards&(c.NumDictShards-1)) != 0 {
		return fmt.Errorf("NumDictShards must be 0 or a power of 2, got %d", c.NumDictShards)
	}

	return nil
}

// DefaultConfig returns a production-ready configuration for 1B nodes.
func DefaultConfig(dataDir string) *Config {
	return &Config{
		DataDir:        dataDir,
		InMemory:       false,
		BlockCacheSize: 8 << 30, // 8GB
		IndexCacheSize: 1 << 30, // 1GB
		LRUCacheSize:   100000,
		Compression:    true,
		SyncWrites:     false,
		NumDictShards:  0, // Use single-threaded encoder by default
	}
}

// TestConfig returns a configuration suitable for testing.
func TestConfig(dataDir string) *Config {
	return &Config{
		DataDir:        dataDir,
		InMemory:       false,
		BlockCacheSize: 100 << 20,   // 100MB
		IndexCacheSize: 10 << 20,    // 10MB
		LRUCacheSize:   1000,
		Compression:    true,
		SyncWrites:     true, // Enable sync writes to ensure persistence in tests
	}
}

// InMemoryConfig returns a configuration for in-memory mode.
func InMemoryConfig() *Config {
	return &Config{
		InMemory:       true,
		BlockCacheSize: 10 << 20, // 10MB
		IndexCacheSize: 1 << 20,  // 1MB
		LRUCacheSize:   1000,
		Compression:    false,
		SyncWrites:     false,
	}
}

// buildBadgerOptions converts Config to badger.Options.
func buildBadgerOptions(cfg *Config) badger.Options {
	opts := badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger"))

	if cfg.InMemory {
		opts = badger.DefaultOptions("")
		opts.InMemory = true
		return opts
	}

	// === Compression ===
	if cfg.Compression {
		// ZSTD provides better compression than Snappy
		opts.Compression = options.ZSTD
	} else {
		opts.Compression = options.None
	}

	// === Cache Sizes ===
	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.IndexCacheSize = cfg.IndexCacheSize

	// Number of compactors (parallel compaction)
	opts.NumCompactors = 4

	// === Bloom Filters ===
	// 1% false positive rate balances memory vs performance
	opts.BloomFalsePositive = 0.01

	// === Value Log ===
	opts.ValueLogFileSize = 256 << 20 // 256MB vlog files

	// === Write Configuration ===
	opts.SyncWrites = cfg.SyncWrites

	return opts
}

// OpenBadgerDB opens a BadgerDB instance with the given configuration.
func OpenBadgerDB(cfg *Config) (*badger.DB, error) {
	opts := buildBadgerOptions(cfg)
	return badger.Open(opts)
}
