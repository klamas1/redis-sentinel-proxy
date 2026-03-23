package proxy

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// TestClassifyPipeError tests the classifyPipeError function with different error types
func TestClassifyPipeError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected PipeErrorType
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: PipeErrorEOF,
		},
		{
			name:     "io.EOF",
			err:      io.EOF,
			expected: PipeErrorEOF,
		},
		{
			name:     "timeout error",
			err:      errors.New("i/o timeout"),
			expected: PipeErrorTimeout,
		},
		{
			name:     "timeout error with 'timeout' string",
			err:      errors.New("context deadline exceeded: timeout"),
			expected: PipeErrorTimeout,
		},
		{
			name:     "connection closed error",
			err:      errors.New("use of closed network connection"),
			expected: PipeErrorConnectionClosed,
		},
		{
			name:     "connection reset by peer",
			err:      errors.New("connection reset by peer"),
			expected: PipeErrorConnectionClosed,
		},
		{
			name:     "broken pipe",
			err:      errors.New("broken pipe"),
			expected: PipeErrorConnectionClosed,
		},
		{
			name:     "network unreachable",
			err:      errors.New("network is unreachable"),
			expected: PipeErrorNetwork,
		},
		{
			name:     "no such host",
			err:      errors.New("no such host"),
			expected: PipeErrorNetwork,
		},
		{
			name:     "connection refused",
			err:      errors.New("connection refused"),
			expected: PipeErrorNetwork,
		},
		{
			name:     "connection timed out",
			err:      errors.New("connection timed out"),
			expected: PipeErrorNetwork,
		},
		{
			name:     "net.Error type",
			err:      &net.OpError{Op: "dial", Net: "tcp", Source: nil, Addr: nil, Err: errors.New("connection failed")},
			expected: PipeErrorNetwork,
		},
		{
			name:     "other error",
			err:      errors.New("some unknown error"),
			expected: PipeErrorOther,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyPipeError(tt.err)
			if result != tt.expected {
				t.Errorf("classifyPipeError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestPipeErrorTypeString tests the String() method of PipeErrorType
func TestPipeErrorTypeString(t *testing.T) {
	tests := []struct {
		errorType PipeErrorType
		expected  string
	}{
		{PipeErrorEOF, "EOF"},
		{PipeErrorTimeout, "Timeout"},
		{PipeErrorConnectionClosed, "ConnectionClosed"},
		{PipeErrorNetwork, "Network"},
		{PipeErrorOther, "Other"},
		{PipeErrorType(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.errorType.String()
			if result != tt.expected {
				t.Errorf("PipeErrorType(%v).String() = %q, want %q", tt.errorType, result, tt.expected)
			}
		})
	}
}

// TestPipeResult tests the PipeResult structure
func TestPipeResult(t *testing.T) {
	err := errors.New("test error")
	result := PipeResult{
		BytesCopied: 1024,
		Error:       err,
		ErrorType:   PipeErrorOther,
	}

	if result.BytesCopied != 1024 {
		t.Errorf("BytesCopied = %d, want 1024", result.BytesCopied)
	}
	if result.Error != err {
		t.Errorf("Error = %v, want %v", result.Error, err)
	}
	if result.ErrorType != PipeErrorOther {
		t.Errorf("ErrorType = %v, want %v", result.ErrorType, PipeErrorOther)
	}
}

// TestPipeWithEOF tests pipe function with normal EOF termination
func TestPipeWithEOF(t *testing.T) {
	reader := strings.NewReader("Hello, World!")
	writer := &closeWriter{}
	sigChan := make(chan struct{}, 1)

	result := pipe(writer, reader, sigChan)

	// Wait for signal
	<-sigChan

	if result.Error != io.EOF && result.Error != nil {
		t.Errorf("pipe() error = %v, want EOF or nil", result.Error)
	}
	if result.BytesCopied != 13 {
		t.Errorf("pipe() bytesCopied = %d, want 13", result.BytesCopied)
	}
	if result.ErrorType != PipeErrorEOF {
		t.Errorf("pipe() errorType = %v, want %v", result.ErrorType, PipeErrorEOF)
	}
	if writer.String() != "Hello, World!" {
		t.Errorf("pipe() wrote %q, want %q", writer.String(), "Hello, World!")
	}
}

// TestPipeWithError tests pipe function with an error
func TestPipeWithError(t *testing.T) {
	// Create a reader that returns an error after some data
	errorReader := &errorReader{data: "some data", returnError: errors.New("forced error")}
	writer := &closeWriter{}
	sigChan := make(chan struct{}, 1)

	result := pipe(writer, errorReader, sigChan)

	// Wait for signal
	<-sigChan

	if result.Error == nil {
		t.Error("pipe() error = nil, want non-nil")
	}
	if result.ErrorType == PipeErrorEOF {
		t.Errorf("pipe() errorType = %v, want non-EOF", result.ErrorType)
	}
}

// errorReader is a custom reader that returns an error after reading some data
type errorReader struct {
	data        string
	read        int
	returnError error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	if r.read > 0 {
		return 0, r.returnError
	}
	n = copy(p, r.data)
	r.read++
	return n, nil
}

// TestPipeTimeout tests pipe function with timeout error
func TestPipeTimeout(t *testing.T) {
	// Create a reader that simulates a timeout by blocking
	blockingReader := &blockingReader{timeout: 100 * time.Millisecond}
	writer := &closeWriter{}
	sigChan := make(chan struct{}, 1)

	// Run pipe in a goroutine since it will block
	done := make(chan PipeResult, 1)
	go func() {
		result := pipe(writer, blockingReader, sigChan)
		done <- result
	}()

	select {
	case result := <-done:
		// Check that we got a result (either timeout or other error)
		if result.Error == nil {
			t.Log("Note: pipe completed without error (blockingReader may have returned EOF)")
		}
		t.Logf("pipe() completed with errorType=%v, error=%v", result.ErrorType, result.Error)
	case <-time.After(2 * time.Second):
		t.Error("pipe() timed out waiting for completion")
	}
}

// blockingReader is a reader that blocks until timeout
type blockingReader struct {
	timeout time.Duration
}

func (r *blockingReader) Read(p []byte) (n int, err error) {
	time.Sleep(r.timeout)
	return 0, io.EOF
}

// closeWriter wraps a bytes.Buffer to implement io.WriteCloser
type closeWriter struct {
	bytes.Buffer
}

func (c *closeWriter) Close() error {
	return nil
}

// TestClassifyPipeErrorWithNetError tests classification of net.Error types
func TestClassifyPipeErrorWithNetError(t *testing.T) {
	// Test with actual net.Error implementations
	dialError := &net.OpError{
		Op:   "dial",
		Net:  "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379},
		Err:  errors.New("connection refused"),
	}

	result := classifyPipeError(dialError)
	if result != PipeErrorNetwork {
		t.Errorf("classifyPipeError(dial error) = %v, want %v", result, PipeErrorNetwork)
	}

	// Test with timeout net.Error
	// Note: String matching for "timeout" takes precedence over net.Error check
	// This is intentional - we want to classify timeout errors specifically
	timeoutError := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: errors.New("i/o timeout"),
	}

	result = classifyPipeError(timeoutError)
	if result != PipeErrorTimeout {
		t.Errorf("classifyPipeError(timeout net.Error) = %v, want %v (timeout string takes precedence)", result, PipeErrorTimeout)
	}

	// Test with net.Error that doesn't have timeout in the message
	otherNetError := &net.OpError{
		Op:  "write",
		Net: "tcp",
		Err: errors.New("connection reset"),
	}

	result = classifyPipeError(otherNetError)
	if result != PipeErrorNetwork {
		t.Errorf("classifyPipeError(other net.Error) = %v, want %v", result, PipeErrorNetwork)
	}
}

// TestPipeResultZeroValues tests PipeResult with zero values
func TestPipeResultZeroValues(t *testing.T) {
	var result PipeResult

	if result.BytesCopied != 0 {
		t.Errorf("Zero value BytesCopied = %d, want 0", result.BytesCopied)
	}
	if result.Error != nil {
		t.Errorf("Zero value Error = %v, want nil", result.Error)
	}
	if result.ErrorType != PipeErrorEOF { // iota starts at 0
		t.Errorf("Zero value ErrorType = %v, want %v", result.ErrorType, PipeErrorEOF)
	}
}
