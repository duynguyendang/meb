package vector

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/duynguyendang/meb/keys"
)

func TestPadAndTransformGolden(t *testing.T) {
	cfg := DefaultHybridConfig()
	paddedDim := nextPow2(1536)

	input := make([]float32, 1536)
	for i := range input {
		input[i] = float32(i) / float32(1536)
	}

	before := QuantizeHybrid(input, cfg)
	cfg.ensureDerived(1536)
	after := QuantizeHybrid(input, cfg)

	if !bytes.Equal(before, after) {
		t.Fatal("PadAndTransform refactor changed output")
	}

	transformed := PadAndTransform(input, paddedDim)
	if len(transformed) != paddedDim {
		t.Fatalf("PadAndTransform length = %d, want %d", len(transformed), paddedDim)
	}

	var inputSq float64
	for _, v := range input {
		inputSq += float64(v * v)
	}
	var transformedSq float64
	for _, v := range transformed {
		transformedSq += float64(v * v)
	}
	if math.Abs(inputSq-transformedSq) > 0.5 {
		t.Logf("PadAndTransform norm: input=%.2f, transformed=%.2f", inputSq, transformedSq)
	}
}

func TestDefaultIVFPQConfig(t *testing.T) {
	cfg := DefaultIVFPQConfig()
	if cfg.NumCentroids != 1024 {
		t.Errorf("NumCentroids = %d, want 1024", cfg.NumCentroids)
	}
	if cfg.NumSubSpaces != 64 {
		t.Errorf("NumSubSpaces = %d, want 64", cfg.NumSubSpaces)
	}
	if cfg.NProbe != 32 {
		t.Errorf("NProbe = %d, want 32", cfg.NProbe)
	}
}

func TestIVFPQConfigValidate(t *testing.T) {
	cfg := DefaultIVFPQConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should pass: %v", err)
	}

	bad := &IVFPQConfig{NumCentroids: 0, NumSubSpaces: 1, BitsPerSpace: 8, NProbe: 1}
	if err := bad.Validate(); err == nil {
		t.Error("expected error for NumCentroids=0")
	}

	bad2 := &IVFPQConfig{NumCentroids: 1, NumSubSpaces: 0, BitsPerSpace: 8, NProbe: 1}
	if err := bad2.Validate(); err == nil {
		t.Error("expected error for NumSubSpaces=0")
	}
}

func TestTrainingBuffer(t *testing.T) {
	buf := NewTrainingBuffer(5)
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer")
	}

	buf.Add([]float32{1, 2, 3})
	if buf.Len() != 1 {
		t.Fatalf("expected 1 item, got %d", buf.Len())
	}

	for i := 0; i < 10; i++ {
		buf.Add([]float32{float32(i)})
	}
	if buf.Len() != 5 {
		t.Fatalf("expected 5 items after overflow, got %d", buf.Len())
	}

	drained := buf.Drain()
	if len(drained) != 5 {
		t.Fatalf("expected 5 items in drain, got %d", len(drained))
	}
	if buf.Len() != 0 {
		t.Fatalf("expected 0 after drain, got %d", buf.Len())
	}
}

func TestSquaredDist(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	dist := squaredDist(a, b)
	expected := float32(27)
	if dist != expected {
		t.Errorf("squaredDist = %f, want %f", dist, expected)
	}
}

func TestFindClosestCentroid(t *testing.T) {
	idx := &IVFPQIndex{
		cfg:        &IVFPQConfig{NumCentroids: 3},
		paddedDim:  4,
		subspaceDim: 2,
	}
	centroids := []float32{
		1, 1, 1, 1,
		10, 10, 10, 10,
		100, 100, 100, 100,
	}
	vec := []float32{9, 9, 9, 9}
	cID := idx.findClosestCentroid(vec, centroids)
	if cID != 1 {
		t.Errorf("expected centroid 1, got %d", cID)
	}
}

func TestADCComputation(t *testing.T) {
	idx := &IVFPQIndex{
		cfg:        &IVFPQConfig{NumSubSpaces: 2, NumCentroids: 2},
		paddedDim:  4,
		subspaceDim: 2,
	}
	lut := make([]float32, 2*256)
	lut[0*256+0] = 0.1
	lut[0*256+1] = 0.2
	lut[1*256+0] = 0.3
	lut[1*256+1] = 0.4
	codes := []byte{0, 1}
	dist := idx.adcDistance(lut, codes)
	expected := lut[0*256+0] + lut[1*256+1]
	if dist != expected {
		t.Errorf("adcDistance = %f, want %f", dist, expected)
	}
}

func TestSelectIndex(t *testing.T) {
	if SelectIndex(100, false) != IndexBruteForce {
		t.Error("expected IndexBruteForce for small corpus")
	}
	if SelectIndex(1_000_000, true) != IndexHNSW {
		t.Error("expected IndexHNSW for large corpus with preferHNSW")
	}
	if SelectIndex(20_000_000, true) != IndexIVFPQ {
		t.Error("expected IndexIVFPQ for very large corpus")
	}
}

func newInMemoryVectorBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("badger.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestIVFPQSearchResidualRecall(t *testing.T) {
	db := newInMemoryVectorBadger(t)
	cfg := &IVFPQConfig{
		NumCentroids: 2,
		NumSubSpaces: 4,
		BitsPerSpace: 8,
		NProbe:       2,
		NSample:      500,
		BatchSize:    100,
	}
	idx := NewIVFPQIndex(db, cfg, 8)
	const topicID uint32 = 1

	// Insert two well-separated clusters:
	// Cluster 0 around (0, ..., 0), Cluster 1 around (50, ..., 50).
	err := db.Update(func(txn *badger.Txn) error {
		for i := 1; i <= 150; i++ {
			v := make([]float32, 8)
			v[0] = float32(i) * 0.01
			if err := idx.AddVector(txn, topicID, uint64(i), v); err != nil {
				return err
			}
		}
		for i := 151; i <= 300; i++ {
			v := make([]float32, 8)
			for d := range v {
				v[d] = 50.0
			}
			v[0] += float32(i-151) * 0.05
			if err := idx.AddVector(txn, topicID, uint64(i), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}

	if err := idx.Train(topicID, 500); err != nil {
		t.Fatalf("Train failed: %v", err)
	}

	query := []float32{50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0}
	var results []SearchResult
	err = db.View(func(txn *badger.Txn) error {
		for res, err := range idx.SearchInTopic(context.Background(), txn, topicID, query, 3) {
			if err != nil {
				return err
			}
			results = append(results, res)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("SearchInTopic failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected non-empty search results")
	}

	top := results[0]
	localID := keys.UnpackLocalID(top.ID)
	if localID < 151 || localID > 155 {
		t.Errorf("expected top-1 localID near 151, got %d (score=%.6f)", localID, top.Score)
	}
	// With residual ADC math, distance to centroid+residual is near 0 so score should be > 0.5.
	// Without centroid subtraction, distance is ~20000 and score is ~0.00005.
	if top.Score < 0.5 {
		t.Errorf("expected high residual ADC score (>= 0.5), got %.6f", top.Score)
	}
}

func TestIVFPQSearchCancellation(t *testing.T) {
	db := newInMemoryVectorBadger(t)
	cfg := &IVFPQConfig{
		NumCentroids: 2,
		NumSubSpaces: 4,
		BitsPerSpace: 8,
		NProbe:       2,
		NSample:      300,
		BatchSize:    50,
	}
	idx := NewIVFPQIndex(db, cfg, 8)
	const topicID uint32 = 1

	err := db.Update(func(txn *badger.Txn) error {
		for i := 1; i <= 20; i++ {
			v := make([]float32, 8)
			v[0] = float32(i)
			if err := idx.AddVector(txn, topicID, uint64(i), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}
	if err := idx.Train(topicID, 300); err != nil {
		t.Fatalf("Train failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var gotErr error
	_ = db.View(func(txn *badger.Txn) error {
		for _, err := range idx.SearchInTopic(ctx, txn, topicID, make([]float32, 8), 5) {
			if err != nil {
				gotErr = err
				break
			}
		}
		return nil
	})
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", gotErr)
	}
}

func TestIVFPQ_NoRawVectorsByDefault(t *testing.T) {
	cfg := DefaultIVFPQConfig()
	if cfg.StoreRawVectors {
		t.Fatal("expected DefaultIVFPQConfig().StoreRawVectors to be false by default")
	}

	db := newInMemoryVectorBadger(t)
	idx := NewIVFPQIndex(db, &IVFPQConfig{
		NumCentroids:    2,
		NumSubSpaces:    4,
		BitsPerSpace:    8,
		NProbe:          2,
		NSample:         500,
		BatchSize:       100,
		StoreRawVectors: false,
	}, 8)
	const topicID uint32 = 1

	err := db.Update(func(txn *badger.Txn) error {
		for i := 1; i <= 40; i++ {
			v := make([]float32, 8)
			if i <= 20 {
				v[0] = float32(i) * 0.1
			} else {
				for d := range v {
					v[d] = 20.0 + float32(i)*0.01
				}
			}
			if err := idx.AddVector(txn, topicID, uint64(i), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}

	// Verify zero 0x16 raw vector keys were written.
	var rawCount int
	_ = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		pfx := []byte{keys.IVFRawVectorPrefix}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			rawCount++
		}
		return nil
	})
	if rawCount != 0 {
		t.Fatalf("expected 0 0x16 raw vector keys when StoreRawVectors=false, got %d", rawCount)
	}

	if err := idx.Train(topicID, 500); err != nil {
		t.Fatalf("Train failed: %v", err)
	}

	// Also verify training from compressed 0x11 keys directly when trainingBuf is empty.
	reg := NewRegistry(db, &Config{
		FullDim:              8,
		HybridBitWidth:       8,
		HybridBlockSize:      32,
		NumWorkers:           2,
		VectorCapacity:       100,
		InitialCapacity:      100,
		SegmentDir:           t.TempDir(),
		SegmentSize:          1 << 20,
		AutoPromoteThreshold: -1,
	})
	defer reg.Close()

	const topic2 uint32 = 2
	for i := 1; i <= 30; i++ {
		v := make([]float32, 8)
		v[0] = float32(i)
		if err := reg.Add(keys.PackID(topic2, uint64(i)), v); err != nil {
			t.Fatalf("reg.Add: %v", err)
		}
	}
	idxFrom0x11 := NewIVFPQIndex(db, &IVFPQConfig{
		NumCentroids:    2,
		NumSubSpaces:    4,
		BitsPerSpace:    8,
		NProbe:          2,
		NSample:         500,
		BatchSize:       100,
		StoreRawVectors: false,
	}, 8)
	if err := idxFrom0x11.Train(topic2, 500); err != nil {
		t.Fatalf("Train from 0x11 failed: %v", err)
	}

	var results []SearchResult
	_ = db.View(func(txn *badger.Txn) error {
		for res, err := range idxFrom0x11.SearchInTopic(context.Background(), txn, topic2, []float32{30, 0, 0, 0, 0, 0, 0, 0}, 3) {
			if err != nil {
				return err
			}
			results = append(results, res)
		}
		return nil
	})
	if len(results) == 0 {
		t.Fatal("expected results when training/reindexing solely from 0x11")
	}
}

func TestIVFPQ_StoreRawVectorsOptIn(t *testing.T) {
	db := newInMemoryVectorBadger(t)
	idx := NewIVFPQIndex(db, &IVFPQConfig{
		NumCentroids:    2,
		NumSubSpaces:    4,
		BitsPerSpace:    8,
		NProbe:          2,
		NSample:         100,
		BatchSize:       50,
		StoreRawVectors: true,
	}, 8)

	err := db.Update(func(txn *badger.Txn) error {
		return idx.AddVector(txn, 1, 1, []float32{1, 2, 3, 4, 5, 6, 7, 8})
	})
	if err != nil {
		t.Fatalf("AddVector: %v", err)
	}

	var rawCount int
	_ = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		pfx := []byte{keys.IVFRawVectorPrefix}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			rawCount++
		}
		return nil
	})
	if rawCount != 1 {
		t.Errorf("expected 1 0x16 key when StoreRawVectors=true, got %d", rawCount)
	}
}
