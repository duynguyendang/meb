package meb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/duynguyendang/meb/store"
	"go.uber.org/goleak"
)

func newTestStoreWithConfig(t *testing.T, cfg *store.Config) *MEBStore {
	t.Helper()
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newTestStoreForScan(t *testing.T) *MEBStore {
	t.Helper()
	segDir := filepath.Join(t.TempDir(), "vectors")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("failed to create segment dir: %v", err)
	}
	cfg := &store.Config{
		DataDir:        "",
		DictDir:        "",
		InMemory:       true,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
		LRUCacheSize:   100,
		Profile:        "Ingest-Heavy",
		SegmentDir:     segDir,
	}
	return newTestStoreWithConfig(t, cfg)
}

func TestScanLegacyCoercion(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	s := newTestStoreForScan(t)

	err := s.AddFact(Fact{Subject: "doc", Predicate: "age", Object: "42"})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("doc", "age", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(int64)
		if !ok {
			t.Fatalf("expected int64, got %T (%v)", f.Object, f.Object)
		}
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	}
}

func TestScanPreserveTypes(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	segDir := filepath.Join(t.TempDir(), "vectors")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("failed to create segment dir: %v", err)
	}
	cfg := &store.Config{
		DataDir:             "",
		DictDir:             "",
		InMemory:            true,
		BlockCacheSize:      1 << 20,
		IndexCacheSize:      1 << 20,
		LRUCacheSize:        100,
		Profile:             "Ingest-Heavy",
		SegmentDir:          segDir,
		PreserveObjectTypes: true,
	}
	s := newTestStoreWithConfig(t, cfg)

	err := s.AddFact(Fact{Subject: "doc", Predicate: "age", Object: "42"})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for f, err := range s.Scan("doc", "age", "") {
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		v, ok := f.Object.(string)
		if !ok {
			t.Fatalf("expected string, got %T (%v)", f.Object, f.Object)
		}
		if v != "42" {
			t.Errorf("expected %q, got %q", "42", v)
		}
	}
}

func TestInlineFloat32Unaffected(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	for _, preserveTypes := range []bool{false, true} {
		t.Run(fmt.Sprintf("preserve=%v", preserveTypes), func(t *testing.T) {
			t.Cleanup(func() { goleak.VerifyNone(t) })

			segDir := filepath.Join(t.TempDir(), "vectors")
			if err := os.MkdirAll(segDir, 0755); err != nil {
				t.Fatalf("failed to create segment dir: %v", err)
			}
			cfg := &store.Config{
				DataDir:             "",
				DictDir:             "",
				InMemory:            true,
				BlockCacheSize:      1 << 20,
				IndexCacheSize:      1 << 20,
				LRUCacheSize:        100,
				Profile:             "Ingest-Heavy",
				SegmentDir:          segDir,
				PreserveObjectTypes: preserveTypes,
			}
			s := newTestStoreWithConfig(t, cfg)

			err := s.AddFact(Fact{Subject: "pi", Predicate: "value", Object: float32(3.14)})
			if err != nil {
				t.Fatalf("AddFact failed: %v", err)
			}

			for f, err := range s.Scan("pi", "value", "") {
				if err != nil {
					t.Fatalf("scan error: %v", err)
				}
				v, ok := f.Object.(float32)
				if !ok {
					t.Fatalf("expected float32, got %T (%v)", f.Object, f.Object)
				}
				diff := v - float32(3.14)
				if diff < 0 {
					diff = -diff
				}
				if diff >= 0.01 {
					t.Errorf("expected ~3.14, got %f", v)
				}
			}
		})
	}
}

func TestDeprecationWarningLoggedOnce(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	// Reset the sync.Once so the warning can fire in this process.
	// NOTE: This modifies a package-level var. Safe only because Go tests
	// within a package run sequentially (not in parallel). If tests ever
	// use t.Parallel(), this will race.
	legacyCoercionWarnOnce = sync.Once{}

	var warnCount int
	var mu sync.Mutex
	h := &countingHandler{
		inner: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}),
		onWarn: func() {
			mu.Lock()
			warnCount++
			mu.Unlock()
		},
	}
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(orig) })

	s := newTestStoreForScan(t)

	err := s.AddFact(Fact{Subject: "doc", Predicate: "val", Object: "100"})
	if err != nil {
		t.Fatalf("AddFact failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		for _, err := range s.Scan("doc", "val", "") {
			if err != nil {
				t.Fatalf("scan error on iteration %d: %v", i, err)
			}
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if warnCount != 1 {
		t.Errorf("expected warn called exactly once, got %d", warnCount)
	}
}

// countingHandler wraps a slog.Handler and calls onWarn for each Warn-level Record.
type countingHandler struct {
	inner slog.Handler
	onWarn func()
}

func (h *countingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *countingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.onWarn()
	}
	return h.inner.Handle(ctx, r)
}

func (h *countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &countingHandler{inner: h.inner.WithAttrs(attrs), onWarn: h.onWarn}
}

func (h *countingHandler) WithGroup(name string) slog.Handler {
	return &countingHandler{inner: h.inner.WithGroup(name), onWarn: h.onWarn}
}
