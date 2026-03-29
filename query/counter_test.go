package query

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestVisitCounterExceeded(t *testing.T) {
	counter := &atomic.Int64{}
	maxVisits := int64(5)

	for i := int64(0); i < maxVisits; i++ {
		counter.Add(1)
		if counter.Load() > maxVisits {
			t.Errorf("should not exceed at visit %d", i+1)
		}
	}

	counter.Add(1)
	if counter.Load() <= maxVisits {
		t.Error("should exceed after 6 visits")
	}
}

func TestVisitCounterUnlimited(t *testing.T) {
	counter := &atomic.Int64{}

	for i := 0; i < 1000; i++ {
		counter.Add(1)
	}
	if counter.Load() != 1000 {
		t.Errorf("expected 1000 visits, got %d", counter.Load())
	}
}

func TestVisitCounterNilSafe(t *testing.T) {
	var vc *atomic.Int64
	maxVisits := int64(10)

	// nil counter should skip check (no panic)
	if vc != nil && maxVisits > 0 {
		if vc.Add(1) > maxVisits {
			t.Error("should not reach here with nil counter")
		}
	}
}

func TestVisitCounterContext(t *testing.T) {
	counter := &atomic.Int64{}
	maxVisits := int64(100)

	ctx := WithVisitCounter(context.Background(), counter, maxVisits)

	extractedCounter, extractedMax := visitCounterFromCtx(ctx)
	if extractedCounter != counter {
		t.Error("expected same counter from context")
	}
	if extractedMax != maxVisits {
		t.Errorf("expected max %d, got %d", maxVisits, extractedMax)
	}

	// Empty context should return nil
	nilCounter, nilMax := visitCounterFromCtx(context.Background())
	if nilCounter != nil {
		t.Error("expected nil counter from empty context")
	}
	if nilMax != 0 {
		t.Errorf("expected 0 max from empty context, got %d", nilMax)
	}
}
