package proxy

import (
	"sync/atomic"
	"time"
)

// State represents the state of the circuit breaker
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "Closed"
	case Open:
		return "Open"
	case HalfOpen:
		return "HalfOpen"
	default:
		return "Unknown"
	}
}

// Config holds the configuration for a CircuitBreaker
type Config struct {
	FailureThreshold    int           // Number of failures before opening the circuit
	SuccessThreshold    int           // Number of successes in HalfOpen before closing
	Timeout             time.Duration // Time to wait in Open state before transitioning to HalfOpen
	HalfOpenMaxRequests int           // Max requests to allow in HalfOpen state
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() Config {
	return Config{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern for fault tolerance
type CircuitBreaker struct {
	config Config

	// Atomic state tracking
	state              int32 // 0=Closed, 1=Open, 2=HalfOpen
	failureCount       int32
	successCount       int32
	halfOpenRequestCount int32
	lastFailureTime    int64 // Unix timestamp in nanoseconds
}

// NewCircuitBreaker creates a new CircuitBreaker with the given configuration
func NewCircuitBreaker(config Config) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  int32(Closed),
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() State {
	return State(atomic.LoadInt32(&cb.state))
}

// AllowRequest checks if a request should be allowed through
func (cb *CircuitBreaker) AllowRequest() bool {
	state := cb.State()

	switch state {
	case Closed:
		return true

	case Open:
		// Check if timeout has passed since last failure
		lastFailure := atomic.LoadInt64(&cb.lastFailureTime)
		if lastFailure > 0 && time.Since(time.Unix(0, lastFailure)) >= cb.config.Timeout {
			// Transition to HalfOpen
			if atomic.CompareAndSwapInt32(&cb.state, int32(Open), int32(HalfOpen)) {
				atomic.StoreInt32(&cb.halfOpenRequestCount, 0)
				atomic.StoreInt32(&cb.successCount, 0)
				return true
			}
		}
		return false

	case HalfOpen:
		// Allow limited number of requests in HalfOpen state
		currentCount := atomic.LoadInt32(&cb.halfOpenRequestCount)
		if currentCount < int32(cb.config.HalfOpenMaxRequests) {
			atomic.AddInt32(&cb.halfOpenRequestCount, 1)
			return true
		}
		return false
	}

	return false
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	state := cb.State()

	if state == HalfOpen {
		successCount := atomic.AddInt32(&cb.successCount, 1)
		if successCount >= int32(cb.config.SuccessThreshold) {
			// Transition to Closed
			atomic.StoreInt32(&cb.state, int32(Closed))
			atomic.StoreInt32(&cb.failureCount, 0)
			atomic.StoreInt32(&cb.successCount, 0)
			atomic.StoreInt32(&cb.halfOpenRequestCount, 0)
		}
	} else if state == Closed {
		// Reset failure count on success in Closed state
		atomic.StoreInt32(&cb.failureCount, 0)
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	state := cb.State()

	if state == HalfOpen {
		// Any failure in HalfOpen state transitions to Open
		atomic.StoreInt32(&cb.state, int32(Open))
		atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())
		atomic.StoreInt32(&cb.successCount, 0)
		atomic.StoreInt32(&cb.halfOpenRequestCount, 0)
	} else if state == Closed {
		failureCount := atomic.AddInt32(&cb.failureCount, 1)
		if failureCount >= int32(cb.config.FailureThreshold) {
			// Transition to Open
			atomic.StoreInt32(&cb.state, int32(Open))
			atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())
		}
	}
}

// Reset resets the circuit breaker to its initial state
func (cb *CircuitBreaker) Reset() {
	atomic.StoreInt32(&cb.state, int32(Closed))
	atomic.StoreInt32(&cb.failureCount, 0)
	atomic.StoreInt32(&cb.successCount, 0)
	atomic.StoreInt32(&cb.halfOpenRequestCount, 0)
	atomic.StoreInt64(&cb.lastFailureTime, 0)
}

// FailureCount returns the current failure count
func (cb *CircuitBreaker) FailureCount() int {
	return int(atomic.LoadInt32(&cb.failureCount))
}
