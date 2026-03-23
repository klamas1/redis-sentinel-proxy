package pool

import (
	"sync"
	"sync/atomic"
)

// PoolManager manages multiple connection pools for different addresses
// Used primarily for replica pools where addresses can change dynamically
type PoolManager struct {
	config      PoolConfig
	pools       sync.Map // map[string]*ConnectionPool
	poolCount   atomic.Int64
	totalGet    atomic.Int64
	totalPut    atomic.Int64
}

// NewPoolManager creates a new pool manager with the given configuration
func NewPoolManager(config PoolConfig) *PoolManager {
	return &PoolManager{
		config: config,
	}
}

// GetPool returns or creates a connection pool for the given address
func (m *PoolManager) GetPool(addr string) *ConnectionPool {
	if addr == "" {
		return nil
	}

	// Try to get existing pool
	if pool, ok := m.pools.Load(addr); ok {
		return pool.(*ConnectionPool)
	}

	// Create new pool with double-check locking
	pool := NewConnectionPool(addr, m.config)

	if existing, loaded := m.pools.LoadOrStore(addr, pool); loaded {
		// Another goroutine created the pool first
		return existing.(*ConnectionPool)
	}

	m.poolCount.Add(1)
	return pool
}

// RemovePool removes and closes the pool for the given address
func (m *PoolManager) RemovePool(addr string) {
	if pool, ok := m.pools.LoadAndDelete(addr); ok {
		pool.(*ConnectionPool).Close()
		m.poolCount.Add(-1)
	}
}

// GetOrCreatePool gets an existing pool or creates a new one
// Returns the pool and a boolean indicating if it was newly created
func (m *PoolManager) GetOrCreatePool(addr string) (*ConnectionPool, bool) {
	if addr == "" {
		return nil, false
	}

	// Try to get existing pool
	if pool, ok := m.pools.Load(addr); ok {
		return pool.(*ConnectionPool), false
	}

	// Create new pool
	pool := NewConnectionPool(addr, m.config)

	if existing, loaded := m.pools.LoadOrStore(addr, pool); loaded {
		return existing.(*ConnectionPool), false
	}

	m.poolCount.Add(1)
	return pool, true
}

// UpdatePools updates the set of pools based on the current list of addresses
// Closes pools for addresses that are no longer in use
func (m *PoolManager) UpdatePools(currentAddresses []string) {
	// Build a set of current addresses
	currentSet := make(map[string]bool, len(currentAddresses))
	for _, addr := range currentAddresses {
		currentSet[addr] = true
	}

	// Find and close pools for addresses that are no longer in use
	toRemove := make([]string, 0)
	m.pools.Range(func(key, value interface{}) bool {
		addr := key.(string)
		if !currentSet[addr] {
			toRemove = append(toRemove, addr)
		}
		return true
	})

	// Remove old pools
	for _, addr := range toRemove {
		m.RemovePool(addr)
	}
}

// CloseAll closes all managed pools
func (m *PoolManager) CloseAll() {
	m.pools.Range(func(key, value interface{}) bool {
		value.(*ConnectionPool).Close()
		return true
	})
	m.pools.Clear()
	m.poolCount.Store(0)
}

// Stats returns aggregated statistics from all pools
func (m *PoolManager) Stats() map[string]PoolStats {
	stats := make(map[string]PoolStats)
	m.pools.Range(func(key, value interface{}) bool {
		addr := key.(string)
		pool := value.(*ConnectionPool)
		stats[addr] = pool.Stats()
		return true
	})
	return stats
}

// PoolCount returns the number of managed pools
func (m *PoolManager) PoolCount() int64 {
	return m.poolCount.Load()
}

// TotalGet returns the total number of Get operations across all pools
func (m *PoolManager) TotalGet() int64 {
	return m.totalGet.Load()
}

// TotalPut returns the total number of Put operations across all pools
func (m *PoolManager) TotalPut() int64 {
	return m.totalPut.Load()
}

// GetPoolCount returns the number of active pools
func (m *PoolManager) GetPoolCount() int64 {
	return m.poolCount.Load()
}
