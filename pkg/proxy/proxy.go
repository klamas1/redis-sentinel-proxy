package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/klamas1/redis-sentinel-proxy/pkg/logger"
	"github.com/klamas1/redis-sentinel-proxy/pkg/metrics"
	"github.com/klamas1/redis-sentinel-proxy/pkg/pool"
	"golang.org/x/sync/errgroup"
)

type resolver interface {
	MasterAddress() string
	ReplicaAddress() string
	DecrementConn(addr string)
	IncrementConn(addr string)
}

// ProxyConfig holds configuration for the proxy
type ProxyConfig struct {
	CircuitBreakerConfig Config
	RetryConfig          RetryConfig
	PoolConfig           pool.PoolConfig
	ConnectTimeout       time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
}

// DefaultProxyConfig returns default configuration
func DefaultProxyConfig() ProxyConfig {
	return ProxyConfig{
		CircuitBreakerConfig: DefaultConfig(),
		RetryConfig:          DefaultRetryConfig(),
		PoolConfig:           pool.DefaultPoolConfig(),
		ConnectTimeout:       1 * time.Second,
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         30 * time.Second,
	}
}

type RedisSentinelProxy struct {
	localAddr *net.TCPAddr
	resolver  resolver
	mode      string // "master" or "replica"
	logger    logger.Logger
	config    ProxyConfig

	// Circuit breakers for each replica (only used in replica mode)
	circuitBreakers map[string]*CircuitBreaker
	cbLock          sync.RWMutex

	// Connection pool manager for replicas
	poolManager *pool.PoolManager
	// Single pool for master connections
	masterPool *pool.ConnectionPool

	// Metrics
	metrics *metrics.Metrics
}

func NewRedisSentinelProxy(localAddr *net.TCPAddr, r resolver, mode string, appLogger logger.Logger, config ProxyConfig, appMetrics *metrics.Metrics) *RedisSentinelProxy {
	proxy := &RedisSentinelProxy{
		localAddr:       localAddr,
		resolver:        r,
		mode:            mode,
		logger:          appLogger,
		config:          config,
		circuitBreakers: make(map[string]*CircuitBreaker),
		poolManager:     pool.NewPoolManager(config.PoolConfig),
		metrics:         appMetrics,
	}

	// Initialize master pool if in master mode
	if mode == "master" {
		// Get initial master address
		masterAddr := r.MasterAddress()
		if masterAddr != "" {
			proxy.masterPool = pool.NewConnectionPool(masterAddr, config.PoolConfig)
		}
	}

	return proxy
}

func (r *RedisSentinelProxy) Run(bigCtx context.Context) error {
	listener, err := net.ListenTCP("tcp", r.localAddr)
	if err != nil {
		return err
	}

	errGr, ctx := errgroup.WithContext(bigCtx)
	errGr.Go(func() error { return r.runListenLoop(ctx, listener) })
	errGr.Go(func() error { return closeListenerByContext(ctx, listener) })

	return errGr.Wait()
}

func (r *RedisSentinelProxy) runListenLoop(ctx context.Context, listener *net.TCPListener) error {
	r.logger.Info("Waiting for connections for proxy %s on %s", strings.ToUpper(r.mode), listener.Addr().String())
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		conn, err := listener.AcceptTCP()
		if err != nil {
			r.logger.Error("Accept error: %s", err)
			continue
		}

		r.logger.Debugf("[DEBUG] New client connection from %s to %s proxy", conn.RemoteAddr().String(), r.mode)

		go r.proxy(ctx, conn)
	}
}

func (r *RedisSentinelProxy) proxy(ctx context.Context, incoming io.ReadWriteCloser) {
	defer incoming.Close()
	var remoteAddr string
	var remote net.Conn
	var err error
	var fromPool bool

	startTime := time.Now()

	if r.mode == "master" {
		// Update master pool address if changed
		r.updateMasterPoolAddress()

		// Get connection from master pool
		remote, err = r.getMasterConnection(ctx)
		if err != nil {
			r.metrics.IncFailedConnections(r.mode)
			r.logger.Errorf("Proxy error: failed to get master connection: %s", err)
			return
		}
		remoteAddr = remote.RemoteAddr().String()
		fromPool = true
	} else {
		// Replica mode with circuit breaker, retry and connection pool
		remote, err = r.connectToReplicaWithCircuitBreaker(ctx)
		if err != nil {
			r.metrics.IncFailedConnections(r.mode)
			r.logger.Errorf("Proxy error: failed to connect to any replica: %s", err)
			return
		}
		remoteAddr = remote.RemoteAddr().String()
		fromPool = true
	}

	// Increment connection count after successful connection
	r.resolver.IncrementConn(remoteAddr)
	defer r.resolver.DecrementConn(remoteAddr)
	defer r.recordConnectionSuccess(remoteAddr)
	defer r.metrics.IncTotalConnections(r.mode)
	defer r.metrics.DecActiveConnections(r.mode)

	r.metrics.IncActiveConnections(r.mode)

	sigChan := make(chan struct{})
	defer close(sigChan)

	// Start both pipe goroutines and collect their results
	var resultFromClient PipeResult
	var resultFromServer PipeResult
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		resultFromClient = pipe(incoming, remote, sigChan)
	}()

	go func() {
		defer wg.Done()
		resultFromServer = pipe(remote, incoming, sigChan)
	}()

	// Wait for both pipes to complete
	wg.Wait()

	// Log connection termination reasons
	r.logger.Debugf("Connection terminated: client->server bytes=%d errorType=%s, server->client bytes=%d errorType=%s",
		resultFromClient.BytesCopied, resultFromClient.ErrorType,
		resultFromServer.BytesCopied, resultFromServer.ErrorType)

	// Return connection to pool if it was from pool and connection was clean
	if fromPool {
		r.returnConnectionToPool(remote, remoteAddr, resultFromClient.ErrorType, resultFromServer.ErrorType)
	}

	// Record metrics
	r.recordConnectionMetrics(resultFromClient, resultFromServer, startTime)
}

// updateMasterPoolAddress updates the master pool if the address has changed
func (r *RedisSentinelProxy) updateMasterPoolAddress() {
	if r.masterPool == nil {
		masterAddr := r.resolver.MasterAddress()
		if masterAddr != "" {
			r.masterPool = pool.NewConnectionPool(masterAddr, r.config.PoolConfig)
			r.logger.Debugf("[DEBUG] Created new master pool for %s", masterAddr)
		}
		return
	}

	currentAddr := r.resolver.MasterAddress()
	if currentAddr != "" && currentAddr != r.masterPool.Address() {
		// Address changed, close old pool and create new one
		r.masterPool.Close()
		r.masterPool = pool.NewConnectionPool(currentAddr, r.config.PoolConfig)
		r.logger.Debugf("[DEBUG] Updated master pool from %s to %s", r.masterPool.Address(), currentAddr)
	}
}

// getMasterConnection gets a connection from the master pool
func (r *RedisSentinelProxy) getMasterConnection(ctx context.Context) (net.Conn, error) {
	if r.masterPool == nil {
		r.logger.Error("[ERROR] Master pool is not initialized")
		return nil, errors.New("master pool is not initialized")
	}

	// Use retry with backoff for pool Get operation
	var remote net.Conn
	operation := func() error {
		var err error
		remote, err = r.masterPool.Get(ctx)
		return err
	}

	err := RetryWithBackoff(ctx, r.config.RetryConfig, operation)
	if err != nil {
		return nil, err
	}

	stats := r.masterPool.Stats()
	r.logger.Debugf("[DEBUG] Got master connection from pool: active=%d, idle=%d, hits=%d, misses=%d",
		stats.ActiveConnections, stats.IdleConnections, stats.HitCount, stats.MissCount)

	return remote, nil
}

// returnConnectionToPool returns a connection to the appropriate pool
func (r *RedisSentinelProxy) returnConnectionToPool(conn net.Conn, addr string, clientErrType, serverErrType PipeErrorType) {
	// Don't return connection if there was a network error
	if clientErrType == PipeErrorNetwork || serverErrType == PipeErrorNetwork {
		r.logger.Debugf("[DEBUG] Not returning connection to pool due to network error")
		conn.Close()
		return
	}

	if r.mode == "master" && r.masterPool != nil {
		r.masterPool.Put(conn)
	} else {
		pool := r.poolManager.GetPool(addr)
		if pool != nil {
			pool.Put(conn)
		} else {
			conn.Close()
		}
	}
}

// connectToReplicaWithCircuitBreaker connects to a replica using connection pool
func (r *RedisSentinelProxy) connectToReplicaWithCircuitBreaker(ctx context.Context) (net.Conn, error) {
	const maxAttempts = 5
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		remoteAddr := r.resolver.ReplicaAddress()
		if remoteAddr == "" {
			r.logger.Error("[ERROR] No upstream address available for replica")
			return nil, nil
		}

		// Get or create pool for this replica
		pool := r.poolManager.GetPool(remoteAddr)
		if pool == nil {
			lastErr = errors.New("failed to get connection pool")
			continue
		}

		// Get or create circuit breaker for this replica
		cb := r.getCircuitBreaker(remoteAddr)

		// Check if circuit breaker allows the request
		if !cb.AllowRequest() {
			r.logger.Debugf("[DEBUG] Circuit breaker is open for %s, skipping", remoteAddr)
			lastErr = &CircuitBreakerOpenError{Address: remoteAddr}
			continue
		}

		r.logger.Debugf("[DEBUG] Proxy request: trying to get connection from pool for %s (attempt %d/%d)", remoteAddr, attempt+1, maxAttempts)

		// Get connection from pool with retry
		var remote net.Conn
		err := RetryWithBackoff(ctx, r.config.RetryConfig, func() error {
			var getErr error
			remote, getErr = pool.Get(ctx)
			return getErr
		})

		if err == nil {
			r.logger.Info("Successfully got connection from pool for replica %s", remoteAddr)
			cb.RecordSuccess()
			return remote, nil
		}

		lastErr = err
		cb.RecordFailure()
		r.logger.Errorf("Failed to get connection from pool for %s: %s", remoteAddr, err)
	}

	return nil, lastErr
}

func (r *RedisSentinelProxy) getCircuitBreaker(addr string) *CircuitBreaker {
	r.cbLock.RLock()
	cb, exists := r.circuitBreakers[addr]
	r.cbLock.RUnlock()

	if exists {
		return cb
	}

	// Create new circuit breaker
	cb = NewCircuitBreaker(r.config.CircuitBreakerConfig)

	r.cbLock.Lock()
	// Double-check after acquiring write lock
	if existing, exists := r.circuitBreakers[addr]; exists {
		r.cbLock.Unlock()
		return existing
	}
	r.circuitBreakers[addr] = cb
	r.cbLock.Unlock()

	return cb
}

func (r *RedisSentinelProxy) recordConnectionSuccess(addr string) {
	r.cbLock.RLock()
	cb, exists := r.circuitBreakers[addr]
	r.cbLock.RUnlock()

	if exists {
		cb.RecordSuccess()
	}
}

// recordConnectionMetrics records connection metrics for monitoring
func (r *RedisSentinelProxy) recordConnectionMetrics(clientResult, serverResult PipeResult, startTime time.Time) {
	// Record bytes in/out
	r.metrics.AddBytesIn(r.mode, float64(clientResult.BytesCopied))
	r.metrics.AddBytesOut(r.mode, float64(serverResult.BytesCopied))

	// Record error types
	if clientResult.ErrorType == PipeErrorTimeout || serverResult.ErrorType == PipeErrorTimeout {
		r.metrics.IncTimeoutErrors(r.mode)
	}

	if clientResult.ErrorType == PipeErrorNetwork {
		r.metrics.IncConnectionErrors(r.mode, "network")
	}
	if serverResult.ErrorType == PipeErrorNetwork {
		r.metrics.IncConnectionErrors(r.mode, "network")
	}

	if clientResult.ErrorType == PipeErrorConnectionClosed {
		r.metrics.IncConnectionErrors(r.mode, "connection_closed")
	}
	if serverResult.ErrorType == PipeErrorConnectionClosed {
		r.metrics.IncConnectionErrors(r.mode, "connection_closed")
	}

	// Record request duration
	duration := time.Since(startTime)
	r.metrics.ObserveRequestDuration(r.mode, duration)

	// Record pool stats
	if r.mode == "master" && r.masterPool != nil {
		stats := r.masterPool.Stats()
		r.metrics.SetPoolStats(r.mode, "active", float64(stats.ActiveConnections))
		r.metrics.SetPoolStats(r.mode, "idle", float64(stats.IdleConnections))
		r.metrics.SetPoolStats(r.mode, "hits", float64(stats.HitCount))
		r.metrics.SetPoolStats(r.mode, "misses", float64(stats.MissCount))
	}
}

func closeListenerByContext(ctx context.Context, listener *net.TCPListener) error {
	defer listener.Close()
	<-ctx.Done()
	return nil
}

// PipeErrorType represents the type of error that occurred during pipe operation
type PipeErrorType int

const (
	PipeErrorEOF PipeErrorType = iota
	PipeErrorTimeout
	PipeErrorConnectionClosed
	PipeErrorNetwork
	PipeErrorOther
)

func (t PipeErrorType) String() string {
	switch t {
	case PipeErrorEOF:
		return "EOF"
	case PipeErrorTimeout:
		return "Timeout"
	case PipeErrorConnectionClosed:
		return "ConnectionClosed"
	case PipeErrorNetwork:
		return "Network"
	case PipeErrorOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// PipeResult holds the result of a pipe operation
type PipeResult struct {
	BytesCopied int64
	Error       error
	ErrorType   PipeErrorType
}

// classifyPipeError classifies an error into a specific PipeErrorType
func classifyPipeError(err error) PipeErrorType {
	if err == nil {
		return PipeErrorEOF
	}

	if err == io.EOF {
		return PipeErrorEOF
	}

	errStr := err.Error()

	// Check for timeout errors
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "i/o timeout") {
		return PipeErrorTimeout
	}

	// Check for connection closed errors
	if strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") {
		return PipeErrorConnectionClosed
	}

	// Check for network errors
	if _, ok := err.(net.Error); ok {
		return PipeErrorNetwork
	}

	// Check for common network-related error messages
	if strings.Contains(errStr, "network is unreachable") ||
		strings.Contains(errStr, "no such host") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection timed out") {
		return PipeErrorNetwork
	}

	return PipeErrorOther
}

func pipe(w io.WriteCloser, r io.Reader, sigChan chan<- struct{}) PipeResult {
	defer func() { sigChan <- struct{}{} }()
	defer w.Close()

	bytesCopied, err := io.Copy(w, r)
	errorType := classifyPipeError(err)

	result := PipeResult{
		BytesCopied: bytesCopied,
		Error:       err,
		ErrorType:   errorType,
	}

	// Log non-EOF errors
	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
		// This will be logged by the caller with proper logger
		_ = errorType
	}

	return result
}

// CircuitBreakerOpenError is returned when circuit breaker is open
type CircuitBreakerOpenError struct {
	Address string
}

func (e *CircuitBreakerOpenError) Error() string {
	return "circuit breaker is open for " + e.Address
}
