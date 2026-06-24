package vector

import (
	"math"
	"testing"
)

func TestDefaultHNSWConfig(t *testing.T) {
	cfg := DefaultHNSWConfig()
	if cfg.M != 16 {
		t.Errorf("M = %d, want 16", cfg.M)
	}
	if cfg.MMax != 32 {
		t.Errorf("MMax = %d, want 32", cfg.MMax)
	}
	if cfg.EfConstruction != 200 {
		t.Errorf("EfConstruction = %d, want 200", cfg.EfConstruction)
	}
	if cfg.EfSearch != 64 {
		t.Errorf("EfSearch = %d, want 64", cfg.EfSearch)
	}
}

func TestRandomLevel(t *testing.T) {
	idx := &HNSWIndex{
		cfg: &HNSWConfig{
			ML: 1.0 / math.Log(16.0),
		},
	}

	levels := make(map[int]int)
	for i := 0; i < 10000; i++ {
		l := idx.randomLevel()
		levels[l]++
	}

	if len(levels) == 0 {
		t.Fatal("no levels generated")
	}

	total := 0
	for _, count := range levels {
		total += count
	}
	if total != 10000 {
		t.Errorf("expected 10000 samples, got %d", total)
	}

	if levels[0] < 5000 {
		t.Error("expected most nodes at level 0")
	}
}

func TestSelectNeighbors(t *testing.T) {
	idx := &HNSWIndex{cfg: &HNSWConfig{M: 5, MMax: 10}}
	candidates := []uint64{1, 2, 3}
	result := idx.selectNeighbors(candidates, 5, 0)
	if len(result) != 3 {
		t.Errorf("expected 3 neighbors, got %d", len(result))
	}

	candidates2 := []uint64{1, 2, 3, 4, 5, 6, 7}
	result2 := idx.selectNeighbors(candidates2, 5, 0)
	if len(result2) != 5 {
		t.Errorf("expected 5 neighbors, got %d", len(result2))
	}
}

func TestMinHeap2(t *testing.T) {
	h := newMinHeap2(10)
	h.push(nodeDist{id: 3, dist: 3})
	h.push(nodeDist{id: 1, dist: 1})
	h.push(nodeDist{id: 2, dist: 2})

	if h.Len() != 3 {
		t.Fatalf("expected len 3, got %d", h.Len())
	}

	popped := h.pop()
	if popped.id != 1 || popped.dist != 1 {
		t.Errorf("expected {1,1}, got {%d,%f}", popped.id, popped.dist)
	}
}

func TestMaxHeap(t *testing.T) {
	h := newMaxHeap(10)
	h.push(nodeDist{id: 1, dist: 1})
	h.push(nodeDist{id: 3, dist: 3})
	h.push(nodeDist{id: 2, dist: 2})

	if h.Len() != 3 {
		t.Fatalf("expected len 3, got %d", h.Len())
	}

	peeked := h.peek()
	if peeked.id != 3 || peeked.dist != 3 {
		t.Errorf("expected peek {3,3}, got {%d,%f}", peeked.id, peeked.dist)
	}
}

func TestADCAccumScalar(t *testing.T) {
	lut := make([]float32, 2*256)
	lut[0*256+1] = 1.5
	lut[1*256+2] = 2.5
	codes := []byte{1, 2}

	dist := adcAccumImplScalar(lut, codes)
	expected := float32(4.0)
	if dist != expected {
		t.Errorf("adcAccumScalar = %f, want %f", dist, expected)
	}
}
