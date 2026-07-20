package store

import "testing"

func TestMinimumResourceConfig_Validate(t *testing.T) {
	cfg := MinimumResourceConfig(t.TempDir())
	if err := cfg.Validate(); err != nil {
		t.Fatalf("MinimumResourceConfig failed validation: %v", err)
	}

	if cfg.Profile != "Minimum" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "Minimum")
	}
	if cfg.ReadOnly {
		t.Error("ReadOnly = true, want false (Minimum profile stays writable)")
	}
	if cfg.SyncWrites {
		t.Error("SyncWrites = true, want false (WAL provides durability)")
	}
	if cfg.BlockCacheSize != 0 || cfg.IndexCacheSize != 0 {
		t.Errorf("caches should be disabled (0), got block=%d index=%d",
			cfg.BlockCacheSize, cfg.IndexCacheSize)
	}
}

func TestSafeServingConfig_Validate(t *testing.T) {
	// Regression: zero caches previously failed validation, making
	// SafeServingConfig/ReadOnlyConfig unusable with NewMEBStore.
	cfg := SafeServingConfig(t.TempDir())
	if err := cfg.Validate(); err != nil {
		t.Fatalf("SafeServingConfig failed validation: %v", err)
	}

	cfg = ReadOnlyConfig(t.TempDir())
	if err := cfg.Validate(); err != nil {
		t.Fatalf("ReadOnlyConfig failed validation: %v", err)
	}
}

func TestMinimumResourceConfig_BuildOptions(t *testing.T) {
	cfg := MinimumResourceConfig(t.TempDir())
	opts := buildBadgerOptions(cfg)

	if opts.NumVersionsToKeep != 1 {
		t.Errorf("NumVersionsToKeep = %d, want 1", opts.NumVersionsToKeep)
	}
	if opts.ValueLogFileSize != 16<<20 {
		t.Errorf("ValueLogFileSize = %d, want %d", opts.ValueLogFileSize, 16<<20)
	}
	if opts.MemTableSize != 8<<20 {
		t.Errorf("MemTableSize = %d, want %d", opts.MemTableSize, 8<<20)
	}
	if opts.NumMemtables != 2 {
		t.Errorf("NumMemtables = %d, want 2", opts.NumMemtables)
	}
	if opts.ReadOnly {
		t.Error("badger ReadOnly option set, want writable store")
	}
}
