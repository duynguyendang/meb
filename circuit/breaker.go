package circuit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrCircuitOpen    = errors.New("circuit breaker is open")
	ErrQueryTimeout   = errors.New("query timeout exceeded")
	ErrQueryCancelled = errors.New("query was cancelled")
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type Config struct {
	QueryTimeout     time.Duration
	FailureThreshold int
	SuccessThreshold int
	OpenDuration     time.Duration
	MaxJoinResults   int
}

func DefaultConfig() *Config {
	return &Config{
		QueryTimeout:     2 * time.Second,
		FailureThreshold: 5,
		SuccessThreshold: 3,
		OpenDuration:     30 * time.Second,
		MaxJoinResults:   5000,
	}
}

type Metrics struct {
	TotalQueries      int64
	SuccessfulQueries int64
	FailedQueries     int64
	TimeoutQueries    int64
	CancelledQueries  int64
	RejectedQueries   int64
	LastFailureTime   time.Time
	LastError         error
}

type MetricsSnapshot struct {
	State             string  `json:"state"`
	TotalQueries      int64   `json:"total_queries"`
	SuccessfulQueries int64   `json:"successful_queries"`
	FailedQueries     int64   `json:"failed_queries"`
	TimeoutQueries    int64   `json:"timeout_queries"`
	CancelledQueries  int64   `json:"cancelled_queries"`
	RejectedQueries   int64   `json:"rejected_queries"`
	FailureRate       float64 `json:"failure_rate"`
	LastFailureTime   string  `json:"last_failure_time,omitempty"`
	LastError         string  `json:"last_error,omitempty"`
}

type Breaker struct {
	config *Config
	mu     sync.RWMutex

	state           State
	failures        int
	successes       int
	lastFailureTime time.Time

	metrics Metrics
}

func NewBreaker(config *Config) *Breaker {
	if config == nil {
		config = DefaultConfig()
	}
	return &Breaker{
		config: config,
		state:  StateClosed,
	}
}

func (b *Breaker) Execute(fn func() error) error {
	return b.ExecuteContext(context.Background(), fn)
}

func (b *Breaker) ExecuteContext(ctx context.Context, fn func() error) error {
	if !b.canExecute() {
		b.mu.Lock()
		b.metrics.RejectedQueries++
		b.mu.Unlock()
		return ErrCircuitOpen
	}

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, b.config.QueryTimeout)
		defer cancel()
	}

	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				select {
				case errChan <- fmt.Errorf("panic in circuit breaker: %v", r):
				case <-done:
				}
			}
		}()
		select {
		case errChan <- fn():
		case <-done:
		}
	}()

	select {
	case <-ctx.Done():
		close(done) // signal goroutine to stop
		if ctx.Err() == context.DeadlineExceeded {
			b.recordFailure(ErrQueryTimeout)
			return ErrQueryTimeout
		}
		b.recordFailure(ErrQueryCancelled)
		return ErrQueryCancelled
	case err := <-errChan:
		if err != nil {
			b.recordFailure(err)
			return err
		}
		b.recordSuccess()
		return nil
	}
}

func (b *Breaker) ExecuteWithResult(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	if !b.canExecute() {
		b.mu.Lock()
		b.metrics.RejectedQueries++
		b.mu.Unlock()
		return nil, ErrCircuitOpen
	}

	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, b.config.QueryTimeout)
		defer cancel()
	}

	type result struct {
		data interface{}
		err  error
	}
	resultChan := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				select {
				case resultChan <- result{nil, fmt.Errorf("panic in circuit breaker: %v", r)}:
				case <-done:
				}
			}
		}()
		data, err := fn()
		select {
		case resultChan <- result{data, err}:
		case <-done:
		}
	}()

	select {
	case <-ctx.Done():
		close(done) // signal goroutine to stop
		if ctx.Err() == context.DeadlineExceeded {
			b.recordFailure(ErrQueryTimeout)
			return nil, ErrQueryTimeout
		}
		b.recordFailure(ErrQueryCancelled)
		return nil, ErrQueryCancelled
	case res := <-resultChan:
		if res.err != nil {
			b.recordFailure(res.err)
			return nil, res.err
		}
		b.recordSuccess()
		return res.data, nil
	}
}

func (b *Breaker) canExecute() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(b.lastFailureTime) > b.config.OpenDuration {
			b.state = StateHalfOpen
			b.failures = 0
			b.successes = 0
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return true
	}
}

func (b *Breaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics.TotalQueries++
	b.metrics.SuccessfulQueries++

	switch b.state {
	case StateHalfOpen:
		b.successes++
		if b.successes >= b.config.SuccessThreshold {
			b.state = StateClosed
			b.failures = 0
			b.successes = 0
		}
	case StateClosed:
		b.failures = 0
	}
}

func (b *Breaker) recordFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics.TotalQueries++
	b.metrics.FailedQueries++
	b.metrics.LastFailureTime = time.Now()
	b.metrics.LastError = err

	if err == ErrQueryTimeout {
		b.metrics.TimeoutQueries++
	} else if err == ErrQueryCancelled {
		b.metrics.CancelledQueries++
	}

	switch b.state {
	case StateHalfOpen:
		b.state = StateOpen
		b.lastFailureTime = time.Now()
		b.failures = 1
	case StateClosed:
		b.failures++
		b.lastFailureTime = time.Now()
		if b.failures >= b.config.FailureThreshold {
			b.state = StateOpen
		}
	}
}

func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

func (b *Breaker) Metrics() Metrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.metrics
}

func (b *Breaker) MetricsSnapshot() MetricsSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var failureRate float64
	if b.metrics.TotalQueries > 0 {
		failureRate = float64(b.metrics.FailedQueries+b.metrics.TimeoutQueries) / float64(b.metrics.TotalQueries)
	}

	snap := MetricsSnapshot{
		State:             b.state.String(),
		TotalQueries:      b.metrics.TotalQueries,
		SuccessfulQueries: b.metrics.SuccessfulQueries,
		FailedQueries:     b.metrics.FailedQueries,
		TimeoutQueries:    b.metrics.TimeoutQueries,
		CancelledQueries:  b.metrics.CancelledQueries,
		RejectedQueries:   b.metrics.RejectedQueries,
		FailureRate:       failureRate,
	}
	if !b.metrics.LastFailureTime.IsZero() {
		snap.LastFailureTime = b.metrics.LastFailureTime.Format(time.RFC3339)
	}
	if b.metrics.LastError != nil {
		snap.LastError = b.metrics.LastError.Error()
	}
	return snap
}

func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateClosed
	b.failures = 0
	b.successes = 0
	b.metrics = Metrics{}
}

func (b *Breaker) ForceOpen() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateOpen
	b.lastFailureTime = time.Now()
}

func (b *Breaker) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return fmt.Sprintf("CircuitBreaker{state=%s, failures=%d, successes=%d, total=%d}",
		b.state, b.failures, b.successes, b.metrics.TotalQueries)
}
