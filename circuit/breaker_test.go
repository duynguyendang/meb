package circuit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewBreaker(t *testing.T) {
	b := NewBreaker(nil)
	if b.State() != StateClosed {
		t.Errorf("expected state closed, got %s", b.State())
	}
}

func TestExecuteSuccess(t *testing.T) {
	b := NewBreaker(nil)
	called := false
	err := b.Execute(func() error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("function was not called")
	}
	m := b.Metrics()
	if m.SuccessfulQueries != 1 {
		t.Errorf("expected 1 successful query, got %d", m.SuccessfulQueries)
	}
}

func TestExecuteFailure(t *testing.T) {
	b := NewBreaker(nil)
	testErr := errors.New("test error")
	err := b.Execute(func() error {
		return testErr
	})
	if err != testErr {
		t.Errorf("expected test error, got %v", err)
	}
	m := b.Metrics()
	if m.FailedQueries != 1 {
		t.Errorf("expected 1 failed query, got %d", m.FailedQueries)
	}
}

func TestExecuteTimeout(t *testing.T) {
	cfg := &Config{
		QueryTimeout:     50 * time.Millisecond,
		FailureThreshold: 10,
		SuccessThreshold: 3,
		OpenDuration:     1 * time.Second,
		MaxJoinResults:   100,
	}
	b := NewBreaker(cfg)

	err := b.Execute(func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	if err != ErrQueryTimeout {
		t.Errorf("expected timeout error, got %v", err)
	}
}

func TestExecuteContextCancellation(t *testing.T) {
	b := NewBreaker(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.ExecuteContext(ctx, func() error {
		return nil
	})
	if err != ErrQueryCancelled {
		t.Errorf("expected cancelled error, got %v", err)
	}
}

func TestCircuitOpensAfterThreshold(t *testing.T) {
	cfg := &Config{
		QueryTimeout:     1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 1,
		OpenDuration:     100 * time.Millisecond,
		MaxJoinResults:   100,
	}
	b := NewBreaker(cfg)
	testErr := errors.New("fail")

	for i := 0; i < 3; i++ {
		b.Execute(func() error { return testErr })
	}

	if b.State() != StateOpen {
		t.Errorf("expected state open after 3 failures, got %s", b.State())
	}

	err := b.Execute(func() error { return nil })
	if err != ErrCircuitOpen {
		t.Errorf("expected circuit open error, got %v", err)
	}
	m := b.Metrics()
	if m.RejectedQueries != 1 {
		t.Errorf("expected 1 rejected query, got %d", m.RejectedQueries)
	}
}

func TestCircuitHalfOpenAfterDuration(t *testing.T) {
	cfg := &Config{
		QueryTimeout:     1 * time.Second,
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		MaxJoinResults:   100,
	}
	b := NewBreaker(cfg)
	testErr := errors.New("fail")

	for i := 0; i < 2; i++ {
		b.Execute(func() error { return testErr })
	}
	if b.State() != StateOpen {
		t.Fatal("expected state open")
	}

	time.Sleep(60 * time.Millisecond)

	err := b.Execute(func() error { return nil })
	if err != nil {
		t.Errorf("expected success after half-open, got %v", err)
	}
	if b.State() != StateClosed {
		t.Errorf("expected state closed after success in half-open, got %s", b.State())
	}
}

func TestReset(t *testing.T) {
	cfg := &Config{
		QueryTimeout:     1 * time.Second,
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenDuration:     1 * time.Second,
		MaxJoinResults:   100,
	}
	b := NewBreaker(cfg)

	for i := 0; i < 2; i++ {
		b.Execute(func() error { return errors.New("fail") })
	}
	if b.State() != StateOpen {
		t.Fatal("expected state open")
	}

	b.Reset()
	if b.State() != StateClosed {
		t.Errorf("expected state closed after reset, got %s", b.State())
	}
	m := b.Metrics()
	if m.TotalQueries != 0 {
		t.Errorf("expected 0 metrics after reset, got %d", m.TotalQueries)
	}
}

func TestForceOpen(t *testing.T) {
	b := NewBreaker(nil)
	b.ForceOpen()
	if b.State() != StateOpen {
		t.Errorf("expected state open, got %s", b.State())
	}
}

func TestPanicRecovery(t *testing.T) {
	b := NewBreaker(nil)
	err := b.Execute(func() error {
		panic("test panic")
	})
	if err == nil {
		t.Error("expected error from panic")
	}
}

func TestExecuteWithResult(t *testing.T) {
	b := NewBreaker(nil)
	result, err := b.ExecuteWithResult(context.Background(), func() (interface{}, error) {
		return "hello", nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "hello" {
		t.Errorf("expected 'hello', got %v", result)
	}
}

func TestString(t *testing.T) {
	b := NewBreaker(nil)
	s := b.String()
	if s == "" {
		t.Error("expected non-empty string")
	}
}

func TestConcurrentExecute(t *testing.T) {
	cfg := DefaultConfig()
	b := NewBreaker(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Execute(func() error { return nil })
		}()
	}
	wg.Wait()

	m := b.Metrics()
	if m.TotalQueries != 100 {
		t.Errorf("expected 100 total queries, got %d", m.TotalQueries)
	}
}
