// Package circuit provides circuit breaker functionality for query timeout protection
package circuit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
	// ErrQueryTimeout is returned when a query exceeds the timeout
	ErrQueryTimeout = errors.New("query timeout exceeded")
	// ErrQueryCancelled is returned when a query is cancelled
	ErrQueryCancelled = errors.New("query was cancelled")
)

// State represents the state of the circuit breaker
type State int

const (
	// StateClosed means the circuit is closed and requests are allowed
	StateClosed State = iota
	// StateOpen means the circuit is open and requests are rejected
	StateOpen
	// StateHalfOpen means the circuit is testing if the service has recovered
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

// Config holds circuit breaker configuration
type Config struct {
	// QueryTimeout is the maximum duration for a query before it's cancelled
	// Default: 2 seconds (as per CSD guardrails)
	QueryTimeout time.Duration

	// FailureThreshold is the number of consecutive failures before opening the circuit
	// Default: 5
	FailureThreshold int

	// SuccessThreshold is the number of consecutive successes in half-open state to close the circuit
	// Default: 3
	SuccessThreshold int

	// OpenDuration is how long the circuit stays open before transitioning to half-open
	// Default: 30 seconds
	OpenDuration time.Duration

	// MaxJoinResults is the maximum number of intermediate facts allowed in a join
	// Default: 5000 (as per CSD guardrails)
	MaxJoinResults int
}

// DefaultConfig returns the default circuit breaker configuration
func DefaultConfig() *Config {
	return &Config{
		QueryTimeout:     2 * time.Second,
		FailureThreshold: 5,
		SuccessThreshold: 3,
		OpenDuration:     30 * time.Second,
		MaxJoinResults:   5000,
	}
}

// Metrics tracks circuit breaker statistics
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

// Breaker implements the circuit breaker pattern for query protection
type Breaker struct {
	config *Config
	mu     sync.RWMutex

	state           State
	failures        int
	successes       int
	lastFailureTime time.Time

	metrics Metrics
}

// NewBreaker creates a new circuit breaker with the given configuration
func NewBreaker(config *Config) *Breaker {
	if config == nil {
		config = DefaultConfig()
	}
	return &Breaker{
		config: config,
		state:  StateClosed,
	}
}

// Execute runs the given function with circuit breaker protection
// Returns an error if the circuit is open or if the function times out
func (b *Breaker) Execute(fn func() error) error {
	return b.ExecuteContext(context.Background(), fn)
}

// ExecuteContext runs the given function with circuit breaker protection and context
func (b *Breaker) ExecuteContext(ctx context.Context, fn func() error) error {
	// Check if we can proceed
	if !b.canExecute() {
		b.mu.Lock()
		b.metrics.RejectedQueries++
		b.mu.Unlock()
		return ErrCircuitOpen
	}

	// Create a timeout context if one isn't provided with sufficient timeout
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, b.config.QueryTimeout)
		defer cancel()
	}

	// Execute the function in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- fn()
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
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

// ExecuteWithResult runs the given function that returns a result with circuit breaker protection
func (b *Breaker) ExecuteWithResult(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	if !b.canExecute() {
		b.mu.Lock()
		b.metrics.RejectedQueries++
		b.mu.Unlock()
		return nil, ErrCircuitOpen
	}

	// Create a timeout context if one isn't provided with sufficient timeout
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, b.config.QueryTimeout)
		defer cancel()
	}

	// Execute the function in a goroutine
	type result struct {
		data interface{}
		err  error
	}
	resultChan := make(chan result, 1)
	go func() {
		data, err := fn()
		resultChan <- result{data, err}
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
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

// canExecute checks if the circuit allows execution
func (b *Breaker) canExecute() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
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

// recordSuccess records a successful execution
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

// recordFailure records a failed execution
func (b *Breaker) recordFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics.TotalQueries++
	b.metrics.FailedQueries++
	b.metrics.LastFailureTime = time.Now()
	b.metrics.LastError = err

	// Track specific error types
	if err == ErrQueryTimeout {
		b.metrics.TimeoutQueries++
	} else if err == ErrQueryCancelled {
		b.metrics.CancelledQueries++
	}

	switch b.state {
	case StateHalfOpen:
		// A single failure in half-open state opens the circuit
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

// State returns the current state of the circuit breaker
func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// Metrics returns a copy of the current metrics
func (b *Breaker) Metrics() Metrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.metrics
}

// Reset resets the circuit breaker to closed state and clears metrics
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateClosed
	b.failures = 0
	b.successes = 0
	b.metrics = Metrics{}
}

// ForceOpen forces the circuit into open state
func (b *Breaker) ForceOpen() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateOpen
	b.lastFailureTime = time.Now()
}

// String returns a string representation of the circuit breaker state
func (b *Breaker) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return fmt.Sprintf("CircuitBreaker{state=%s, failures=%d, successes=%d, total=%d}",
		b.state, b.failures, b.successes, b.metrics.TotalQueries)
}
