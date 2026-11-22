package vector

import (
	"context"
	"math/rand"
	"testing"

	"github.com/duynguyendang/meb/keys"
	"go.uber.org/goleak"
)

func TestAdaptiveIndex_AutoPromoteLifecycle(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	for _, topicID := range []uint32{0, 1} {
		name := "topic0_default"
		if topicID == 1 {
			name = "topic1_default"
		}
		t.Run(name, func(t *testing.T) {
			db := newInMemoryVectorBadger(t)
			r := NewRegistry(db, &Config{
				FullDim:              32,
				HybridBitWidth:       8,
				HybridBlockSize:      32,
				NumWorkers:           2,
				VectorCapacity:       500,
				InitialCapacity:      200,
				SegmentDir:           t.TempDir(),
				SegmentSize:          1 << 20,
				AutoPromoteThreshold: 60,
			})
			t.Cleanup(func() { _ = r.Close() })

			rng := rand.New(rand.NewSource(99))
			const belowThreshold = 40
			for i := 1; i <= belowThreshold; i++ {
				v := make([]float32, 32)
				for d := range v {
					v[d] = rng.Float32() * 0.2
				}
				v[0] += float32(i%5) * 2.0
				id := keys.PackID(topicID, uint64(i))
				if err := r.Add(id, v); err != nil {
					t.Fatalf("Add(%d): %v", i, err)
				}
			}

			if state := r.TopicIndexState(topicID); state != StateBruteForce {
				t.Fatalf("expected StateBruteForce below threshold, got %v", state)
			}

			// Cross threshold (triggering StateBuilding) and immediately continue ingesting
			// so concurrent writes test the delta buffer and replay into StateANN.
			needle := make([]float32, 32)
			needle[0], needle[1], needle[2], needle[3] = 25, 25, 25, 25
			const totalVecs = 90
			for i := belowThreshold + 1; i <= totalVecs; i++ {
				v := make([]float32, 32)
				for d := range v {
					v[d] = rng.Float32() * 0.2
				}
				if i == totalVecs {
					copy(v, needle)
				} else {
					v[1] += float32(i%7) * 2.0
				}
				id := keys.PackID(topicID, uint64(i))
				if err := r.Add(id, v); err != nil {
					t.Fatalf("Add(%d): %v", i, err)
				}
			}

			r.WaitForAutoPromote()

			if state := r.TopicIndexState(topicID); state != StateANN {
				t.Fatalf("expected StateANN after WaitForAutoPromote, got %v", state)
			}
			if r.PromotedIVFPQIndex(topicID) == nil {
				t.Fatal("expected non-nil PromotedIVFPQIndex")
			}

			// Search both via SearchInTopic and default Search must use promoted IVF-PQ and find needle (id=90).
			var topicResults []SearchResult
			for res, err := range r.SearchInTopic(context.Background(), topicID, needle, 5) {
				if err != nil {
					t.Fatalf("SearchInTopic: %v", err)
				}
				topicResults = append(topicResults, res)
			}
			if len(topicResults) == 0 {
				t.Fatal("expected non-empty results after auto-promotion")
			}
			wantID := keys.PackID(topicID, totalVecs)
			foundNeedle := false
			for _, res := range topicResults {
				if res.ID == wantID {
					foundNeedle = true
					break
				}
			}
			if !foundNeedle {
				t.Errorf("expected late/delta vector %d in top-5 promoted ANN results, got %+v", wantID, topicResults)
			}

			var globalResults []SearchResult
			for res, err := range r.Search(context.Background(), needle, 5) {
				if err != nil {
					t.Fatalf("Search: %v", err)
				}
				globalResults = append(globalResults, res)
			}
			if len(globalResults) == 0 {
				t.Fatal("expected non-empty global Search results in StateANN")
			}
		})
	}
}
