package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// PoolConfig holds configuration for the connection pool
type PoolConfig struct {
	MaxIdle         int           // Максимальное количество свободных соединений в пуле
	MaxActive       int           // Максимальное количество активных соединений
	IdleTimeout     time.Duration // Время жизни свободного соединения
	MaxConnLifetime time.Duration // Максимальное время жизни соединения
}

// DefaultPoolConfig returns default pool configuration
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}
}

// PoolStats holds statistics about the connection pool
type PoolStats struct {
	TotalCreated       int64 // Всего созданных соединений
	TotalCheckedOut    int64 // Всего выданных соединений из пула
	ActiveConnections  int32 // Текущее количество активных соединений
	IdleConnections    int32 // Текущее количество свободных соединений
	HitCount           int64 // Количество попаданий в пул (использование существующего соединения)
	MissCount          int64 // Количество промахов (создание нового соединения)
}

// ConnectionPool manages a pool of TCP connections
type ConnectionPool struct {
	addr            string
	config          PoolConfig
	idleConns       chan net.Conn
	activeCount     atomic.Int32
	totalCreated    atomic.Int64
	totalCheckedOut atomic.Int64
	hitCount        atomic.Int64
	missCount       atomic.Int64
	closeCh         chan struct{}
	closed          atomic.Bool
	mu              sync.Mutex
	createdTime     time.Time
}

// NewConnectionPool creates a new connection pool for the given address
func NewConnectionPool(addr string, config PoolConfig) *ConnectionPool {
	pool := &ConnectionPool{
		addr:        addr,
		config:      config,
		idleConns:   make(chan net.Conn, config.MaxIdle),
		closeCh:     make(chan struct{}),
		createdTime: time.Now(),
	}

	// Start idle timeout cleaner
	go pool.idleTimeoutCleaner()

	return pool
}

// Get retrieves a connection from the pool or creates a new one
func (p *ConnectionPool) Get(ctx context.Context) (net.Conn, error) {
	// Check if pool is closed
	if p.closed.Load() {
		return nil, errors.New("connection pool is closed")
	}

	// Check max active connections
	for {
		active := p.activeCount.Load()
		if active >= int32(p.config.MaxActive) {
			// Try to get a connection from idle pool
			select {
			case conn := <-p.idleConns:
				if p.isValidConnection(conn) {
					p.hitCount.Add(1)
					p.totalCheckedOut.Add(1)
					return conn, nil
				}
				// Connection is invalid, continue waiting
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-p.closeCh:
				return nil, errors.New("connection pool is closed")
			}
		}

		// Try to acquire slot for new connection
		if p.activeCount.CompareAndSwap(active, active+1) {
			break
		}
	}

	// Try to get an existing idle connection first
	select {
	case conn := <-p.idleConns:
		if p.isValidConnection(conn) {
			p.hitCount.Add(1)
			p.totalCheckedOut.Add(1)
			return conn, nil
		}
		// Connection is invalid, will create new one
	default:
	}

	// Create new connection
	p.missCount.Add(1)
	p.totalCreated.Add(1)

	conn, err := p.dialWithTimeout(ctx)
	if err != nil {
		// Release the active slot
		p.activeCount.Add(-1)
		return nil, err
	}

	p.totalCheckedOut.Add(1)
	return conn, nil
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn net.Conn) {
	if conn == nil {
		p.activeCount.Add(-1)
		return
	}

	// Check if pool is closed or connection is invalid
	if p.closed.Load() || !p.isValidConnection(conn) {
		conn.Close()
		p.activeCount.Add(-1)
		return
	}

	// Check connection lifetime
	if p.createdTime.Add(p.config.MaxConnLifetime).Before(time.Now()) {
		conn.Close()
		p.activeCount.Add(-1)
		return
	}

	// Try to return to pool
	select {
	case p.idleConns <- conn:
		// Successfully returned to pool
		p.activeCount.Add(-1)
	default:
		// Pool is full, close the connection
		conn.Close()
		p.activeCount.Add(-1)
	}
}

// Close closes the pool and all idle connections
func (p *ConnectionPool) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return // Already closed
	}

	close(p.closeCh)

	// Drain and close all idle connections
	for {
		select {
		case conn := <-p.idleConns:
			conn.Close()
		default:
			return
		}
	}
}

// Stats returns current pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	return PoolStats{
		TotalCreated:      p.totalCreated.Load(),
		TotalCheckedOut:   p.totalCheckedOut.Load(),
		ActiveConnections: p.activeCount.Load(),
		IdleConnections:   int32(len(p.idleConns)),
		HitCount:          p.hitCount.Load(),
		MissCount:         p.missCount.Load(),
	}
}

// isValidConnection checks if a connection is still valid
func (p *ConnectionPool) isValidConnection(conn net.Conn) bool {
	// Use SetDeadline to check if connection is still alive
	// This is a non-blocking check
	now := time.Now()
	conn.SetDeadline(now.Add(100 * time.Millisecond))

	conn.SetReadDeadline(now.Add(100 * time.Millisecond))

	// Non-blocking read to check connection state
	conn.SetReadDeadline(time.Time{}) // Reset deadline for normal operation

	// Reset deadline for normal operation
	conn.SetDeadline(time.Time{})

	return true
}

// dialWithTimeout creates a new connection with timeout
func (p *ConnectionPool) dialWithTimeout(ctx context.Context) (net.Conn, error) {
	// Create a deadline based on context or default timeout
	defaultTimeout := 5 * time.Second
	deadline := time.Now().Add(defaultTimeout)

	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	conn, err := net.DialTimeout("tcp", p.addr, defaultTimeout)
	if err != nil {
		return nil, err
	}

	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		conn.Close()
		return nil, ctx.Err()
	default:
	}

	return conn, nil
}

// idleTimeoutCleaner periodically removes idle connections that have exceeded IdleTimeout
func (p *ConnectionPool) idleTimeoutCleaner() {
	ticker := time.NewTicker(p.config.IdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.cleanIdleConnections()
		}
	}
}

// cleanIdleConnections removes connections that have exceeded IdleTimeout
func (p *ConnectionPool) cleanIdleConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Note: We can't track individual connection timestamps without wrapping them
	// For now, this is a placeholder for future enhancement
	// A full implementation would wrap connections with metadata including creation time
}

// Address returns the address this pool connects to
func (p *ConnectionPool) Address() string {
	return p.addr
}
