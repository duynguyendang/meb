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

	// DictDir is the directory where the Dictionary BadgerDB will store its data.
	DictDir string

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

	// MemTableSize is the size of the memtable in bytes.
	// Default: 64MB. Lower this to reduce memory usage.
	MemTableSize int64

	// NumMemtables is the maximum number of tables to keep in memory which are waiting to be written to disk.
	// Default: 5. Lower this to reduce memory usage.
	NumMemtables int

	// Profile specifies the resource profile ("Ingest-Heavy", "Safe-Serving").
	// Defaults to "Ingest-Heavy" if empty.
	Profile string

	// ReadOnly enables read-only mode.
	ReadOnly bool
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
		DictDir:        filepath.Join(dataDir, "dict"), // Separate dictionary directory
		InMemory:       false,
		BlockCacheSize: 8 << 30, // 8GB
		IndexCacheSize: 2 << 30, // 2GB (Default for Ingest-Heavy)
		LRUCacheSize:   100000,
		Compression:    true,
		SyncWrites:     false,
		NumDictShards:  0,              // Use single-threaded encoder by default
		Profile:        "Ingest-Heavy", // Default profile
	}
}

// buildBadgerOptions converts Config to badger.Options based on Profile.
func buildBadgerOptions(cfg *Config) badger.Options {
	opts := badger.DefaultOptions(filepath.Join(cfg.DataDir, "badger"))

	if cfg.InMemory {
		opts = badger.DefaultOptions("")
		opts.InMemory = true
		return opts
	}

	// === Common Settings ===
	// Disable conflict detection as we handle logic at app layer (SPO/OPS)
	opts.DetectConflicts = false

	// 1% false positive rate balances memory vs performance
	opts.BloomFalsePositive = 0.01

	// === Compression ===
	if cfg.Compression {
		opts.Compression = options.ZSTD
	} else {
		opts.Compression = options.None
	}

	// === Profile Specific Settings ===
	switch cfg.Profile {
	case "Safe-Serving":
		// Optimized for Low RAM / WSL / Cloud Run (1GB - 2GB Env)

		// Note: TableLoadingMode/ValueLogLoadingMode APIs are not available in this Badger v4 version.
		// We rely on default behavior (mmap).
		// Use small ValueLogSize to minimize mmap overhead/locking.

		// Strict small ValueLog to keep IO stable
		opts.ValueLogFileSize = 64 << 20 // 64MB

		// Limit compactors to prevent IO saturation on shared/emulated drives
		// Badger v4 requires at least 2 compactors.
		opts.NumCompactors = 2

		// Force ReadOnly if configured (prevents compactions completely)
		if cfg.ReadOnly {
			opts.ReadOnly = true
		}

	case "Ingest-Heavy":
		fallthrough
	default:
		// Optimized for High Performance / High RAM (Ingestion)

		// Large ValueLog
		opts.ValueLogFileSize = 1 << 30 // 1GB

		// Standard Compactors
		opts.NumCompactors = 4
	}

	// === Cache Sizes ===
	// We use the values from parameters, which should have been set according to profile preferences
	// by the caller or defaults.
	opts.BlockCacheSize = cfg.BlockCacheSize
	opts.IndexCacheSize = cfg.IndexCacheSize

	// === Write Configuration ===
	opts.SyncWrites = cfg.SyncWrites

	// === Memory Tuning ===
	if cfg.MemTableSize > 0 {
		opts.MemTableSize = cfg.MemTableSize
	}
	if cfg.NumMemtables > 0 {
		opts.NumMemtables = cfg.NumMemtables
	}

	return opts
}

// OpenBadgerDB opens a BadgerDB instance with the given configuration.
func OpenBadgerDB(cfg *Config) (*badger.DB, error) {
	opts := buildBadgerOptions(cfg)
	return badger.Open(opts)
}
