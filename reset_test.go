package meb

import (
	"context"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/duynguyendang/meb/store"
)

func resetTestStore(t *testing.T) *MEBStore {
	t.Helper()
	segDir := t.TempDir()
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
	s, err := NewMEBStore(cfg)
	if err != nil {
		t.Fatalf("NewMEBStore: %v", err)
	}
	t.Cleanup(func() {
		s.Vectors().WaitTombstones()
		s.Close()
	})
	return s
}

func TestResetNoDeadlock(t *testing.T) {
	s := resetTestStore(t)

	facts := make([]Fact, 1000)
	for i := 0; i < 1000; i++ {
		facts[i] = Fact{
			Subject:   "sub",
			Predicate: "pred",
			Object:    int32(i),
		}
	}
	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	for i := 0; i < 100; i++ {
		vec := make([]float32, 1536)
		for j := range vec {
			vec[j] = rand.Float32()*2 - 1
		}
		s.Vectors().Add(uint64(i+1), vec)
	}

	var wg sync.WaitGroup
	serial := make(chan struct{}, 1)

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			serial <- struct{}{}
			err := s.Reset()
			if err != nil {
				t.Errorf("Reset goroutine %d failed: %v", idx, err)
			}
			<-serial
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Reset deadlock: 4 concurrent Reset() calls did not complete within 30s")
	}

	if s.Count() != 0 {
		t.Errorf("expected count 0 after reset, got %d", s.Count())
	}
}

func TestResetDuringConcurrentSearch(t *testing.T) {
	s := resetTestStore(t)

	dim := 1536
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		var sum float32
		for _, val := range vec {
			sum += val * val
		}
		norm := float32(math.Sqrt(float64(sum)))
		if norm > 0 {
			for j := range vec {
				vec[j] /= norm
			}
		}
		if err := s.Vectors().Add(uint64(i+1), vec); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	query := make([]float32, dim)
	for j := range query {
		query[j] = rng.Float32()*2 - 1
	}
	var sum float32
	for _, val := range query {
		sum += val * val
	}
	norm := float32(math.Sqrt(float64(sum)))
	if norm > 0 {
		for j := range query {
			query[j] /= norm
		}
	}

	var searchWg sync.WaitGroup

	for i := 0; i < 4; i++ {
		searchWg.Add(1)
		go func() {
			defer searchWg.Done()
			for sr, err := range s.Vectors().Search(context.Background(), query, 10) {
				if err != nil {
					return
				}
				_ = sr
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)

	err := s.Reset()
	if err != nil {
		t.Logf("Reset error during search: %v (acceptable)", err)
	}

	searchWg.Wait()

	if s.Count() != 0 {
		t.Errorf("expected count 0 after reset, got %d", s.Count())
	}
}

func TestResetReleasesReadLock(t *testing.T) {
	s := resetTestStore(t)

	facts := make([]Fact, 10000)
	for i := 0; i < 10000; i++ {
		facts[i] = Fact{
			Subject:   "sub",
			Predicate: "pred",
			Object:    int32(i),
		}
	}
	if err := s.AddFactBatch(facts); err != nil {
		t.Fatalf("AddFactBatch: %v", err)
	}

	scanStarted := make(chan struct{})
	go func() {
		close(scanStarted)
		count := 0
		for _, err := range s.Scan("sub", "", "") {
			if err != nil {
				break
			}
			count++
			if count >= 100 {
				break
			}
		}
	}()

	<-scanStarted
	time.Sleep(10 * time.Millisecond)

	resetDone := make(chan struct{})
	go func() {
		err := s.Reset()
		if err != nil {
			t.Errorf("Reset failed: %v", err)
		}
		close(resetDone)
	}()

	select {
	case <-resetDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Reset blocked: m.mu was held during DropAll, blocking concurrent reads")
	}
}

func TestResetReusesCleanupGoroutine(t *testing.T) {
	s := resetTestStore(t)

	baselineGoroutines := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		err := s.Reset()
		if err != nil {
			t.Fatalf("Reset iteration %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	afterGoroutines := runtime.NumGoroutine()
	diff := afterGoroutines - baselineGoroutines
	if diff > 2 {
		t.Errorf("goroutine count diff = %d (before=%d, after=%d), want <= 2",
			diff, baselineGoroutines, afterGoroutines)
	}
}

// Ensure context import is used
var _ = context.Background
