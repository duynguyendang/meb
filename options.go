package meb

import "github.com/dgraph-io/badger/v4"

// GetMEBOptions returns BadgerDB options optimized for the host environment.
func GetMEBOptions(isReadOnly bool) badger.Options {
	opts := badger.DefaultOptions("")

	// Detect environment (simplified logic for now)
	// In a real scenario, this would check environment variables or runtime flags.
	// Default to "cloud_run" profile for safety (low memory) if not specified.

	opts.ReadOnly = isReadOnly
	opts.Logger = nil // Disable default logger to avoid noise

	// Profile: Cloud Run (Serving) - Low Memory
	// Optimization: bloom filters off for read-only if valid, small cache
	if isReadOnly {
		opts.IndexCacheSize = 256 << 20 // 256MB
		opts.BlockCacheSize = 64 << 20  // 64MB

	} else {
		// Profile: VM (Ingestion) - High Throughput
		opts.IndexCacheSize = 2 << 30 // 2GB
		opts.BlockCacheSize = 1 << 30 // 1GB
		opts.NumCompactors = 4
		opts.CompactL0OnClose = true
	}

	return opts
}
