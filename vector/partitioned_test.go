package vector

import (
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// newTestPartitionedRegistry creates an in-memory PartitionedRegistry for testing.
func newTestPartitionedRegistry(t *testing.T, maxPartitions int) *PartitionedRegistry {
	t.Helper()

	opts := badger.DefaultOptions("")
	opts.InMemory = true
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory BadgerDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cfg := DefaultConfig()
	cfg.SegmentDir = t.TempDir()
	cfg.InitialCapacity = 10
	cfg.VectorCapacity = 100
	cfg.FullDim = 4            // small dim for fast tests
	cfg.HybridBlockSize = 8    // min block size (must be divisible by 8)
	cfg.NumWorkers = 1         // single worker for determinism

	pr := NewPartitionedRegistry(db, cfg)
	pr.SetMaxPartitions(maxPartitions)
	t.Cleanup(func() { pr.Close() })

	return pr
}

func TestPartitionedRegistry_Add_PartitionLimit(t *testing.T) {
	pr := newTestPartitionedRegistry(t, 2) // only 2 partitions

	// First two topics succeed
	if err := pr.Add(1, 100, []float32{0.1, 0.2, 0.3, 0.4}); err != nil {
		t.Fatalf("Add(topic=1) failed: %v", err)
	}
	if err := pr.Add(2, 101, []float32{0.5, 0.6, 0.7, 0.8}); err != nil {
		t.Fatalf("Add(topic=2) failed: %v", err)
	}

	// Third topic returns ErrPartitionLimit
	err := pr.Add(3, 102, []float32{0.9, 0.1, 0.2, 0.3})
	if err == nil {
		t.Fatal("expected error for topic beyond partition limit, got nil")
	}
	if !errors.Is(err, ErrPartitionLimit) {
		t.Errorf("expected ErrPartitionLimit, got %v", err)
	}
}

func TestPartitionedRegistry_AddWithHash_PartitionLimit(t *testing.T) {
	pr := newTestPartitionedRegistry(t, 2) // only 2 partitions

	// First two topics succeed
	if err := pr.AddWithHash(1, 100, []float32{0.1, 0.2, 0.3, 0.4}, 1); err != nil {
		t.Fatalf("AddWithHash(topic=1) failed: %v", err)
	}
	if err := pr.AddWithHash(2, 101, []float32{0.5, 0.6, 0.7, 0.8}, 2); err != nil {
		t.Fatalf("AddWithHash(topic=2) failed: %v", err)
	}

	// Third topic returns ErrPartitionLimit
	err := pr.AddWithHash(3, 102, []float32{0.9, 0.1, 0.2, 0.3}, 3)
	if err == nil {
		t.Fatal("expected error for topic beyond partition limit, got nil")
	}
	if !errors.Is(err, ErrPartitionLimit) {
		t.Errorf("expected ErrPartitionLimit, got %v", err)
	}
}

func TestPartitionedRegistry_ExistingTopicWorksAtLimit(t *testing.T) {
	pr := newTestPartitionedRegistry(t, 2)

	// Fill both partitions
	if err := pr.Add(1, 100, []float32{0.1, 0.2, 0.3, 0.4}); err != nil {
		t.Fatalf("Add(topic=1) failed: %v", err)
	}
	if err := pr.Add(2, 101, []float32{0.5, 0.6, 0.7, 0.8}); err != nil {
		t.Fatalf("Add(topic=2) failed: %v", err)
	}

	// Adding to an existing topic still works (doesn't create a new partition)
	if err := pr.Add(1, 200, []float32{0.9, 0.1, 0.2, 0.3}); err != nil {
		t.Fatalf("Add to existing topic failed at limit: %v", err)
	}
	if err := pr.Add(2, 201, []float32{0.4, 0.5, 0.6, 0.7}); err != nil {
		t.Fatalf("Add to existing topic failed at limit: %v", err)
	}

	// Verify counts
	if pr.CountByTopic(1) != 2 {
		t.Errorf("topic 1 count = %d, want 2", pr.CountByTopic(1))
	}
	if pr.CountByTopic(2) != 2 {
		t.Errorf("topic 2 count = %d, want 2", pr.CountByTopic(2))
	}
	if pr.Count() != 4 {
		t.Errorf("total count = %d, want 4", pr.Count())
	}
}

func TestPartitionedRegistry_NoLimit(t *testing.T) {
	pr := newTestPartitionedRegistry(t, 100) // generous limit

	// Add many topics — all should succeed
	for topicID := uint32(1); topicID <= 10; topicID++ {
		if err := pr.Add(topicID, uint64(topicID), []float32{0.1, 0.2, 0.3, 0.4}); err != nil {
			t.Fatalf("Add(topic=%d) failed: %v", topicID, err)
		}
	}

	if pr.Count() != 10 {
		t.Errorf("total count = %d, want 10", pr.Count())
	}
}
