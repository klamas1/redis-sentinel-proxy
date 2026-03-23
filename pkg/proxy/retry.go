package proxy

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// RetryConfig holds the configuration for retry with exponential backoff
type RetryConfig struct {
	MaxRetries    int           // Maximum number of retry attempts
	InitialDelay  time.Duration // Initial delay before first retry
	MaxDelay      time.Duration // Maximum delay between retries
	Multiplier    float64       // Multiplier for exponential backoff
	JitterFactor  float64       // Jitter factor (0.0 to 1.0)
}

// DefaultRetryConfig returns a sensible default configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
		JitterFactor: 0.1,
	}
}

// CalculateDelay calculates the delay for a given attempt using exponential backoff with jitter
// Formula: delay = min(InitialDelay * (Multiplier ^ attempt) * (1 + Jitter), MaxDelay)
func CalculateDelay(config RetryConfig, attempt int) time.Duration {
	// Calculate exponential backoff
	exponentialDelay := float64(config.InitialDelay) * math.Pow(config.Multiplier, float64(attempt))

	// Add jitter: random value between (1 - JitterFactor) and (1 + JitterFactor)
	jitter := 1.0 + (rand.Float64()*2.0-1.0)*config.JitterFactor
	delayWithJitter := exponentialDelay * jitter

	// Cap at max delay
	delay := time.Duration(math.Min(delayWithJitter, float64(config.MaxDelay)))

	return delay
}

// RetryWithBackoff executes an operation with exponential backoff retry logic
// It respects context cancellation and returns the first error if all retries are exhausted
func RetryWithBackoff(ctx context.Context, config RetryConfig, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check if context is cancelled before each attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute the operation
		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if this was the last attempt
		if attempt >= config.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff and jitter
		delay := CalculateDelay(config, attempt)

		// Wait for the delay, but respect context cancellation
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Continue to next retry
		}
	}

	return lastErr
}

// RetryWithBackoffAndResult is like RetryWithBackoff but for operations that return a value and an error
func RetryWithBackoffAndResult[T any](ctx context.Context, config RetryConfig, operation func() (T, error)) (T, error) {
	var result T
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check if context is cancelled before each attempt
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		// Execute the operation
		result, err := operation()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry if this was the last attempt
		if attempt >= config.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff and jitter
		delay := CalculateDelay(config, attempt)

		// Wait for the delay, but respect context cancellation
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
			// Continue to next retry
		}
	}

	return result, lastErr
}
