package pool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// mockListener creates a mock TCP listener for testing
func mockListener(tb testing.TB) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("Failed to create listener: %v", err)
	}
	return listener
}

// TestDefaultPoolConfig tests default configuration values
func TestDefaultPoolConfig(t *testing.T) {
	config := DefaultPoolConfig()

	if config.MaxIdle != 10 {
		t.Errorf("Expected MaxIdle to be 10, got %d", config.MaxIdle)
	}
	if config.MaxActive != 100 {
		t.Errorf("Expected MaxActive to be 100, got %d", config.MaxActive)
	}
	if config.IdleTimeout != 30*time.Second {
		t.Errorf("Expected IdleTimeout to be 30s, got %v", config.IdleTimeout)
	}
	if config.MaxConnLifetime != 5*time.Minute {
		t.Errorf("Expected MaxConnLifetime to be 5m, got %v", config.MaxConnLifetime)
	}
}

// TestConnectionPool_GetAndPut tests basic Get and Put operations
func TestConnectionPool_GetAndPut(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	// Get a connection from the pool
	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer pool.Put(conn)

	if conn == nil {
		t.Fatal("Expected non-nil connection")
	}

	// Check stats
	stats := pool.Stats()
	if stats.ActiveConnections != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.ActiveConnections)
	}
	if stats.TotalCreated != 1 {
		t.Errorf("Expected 1 total created, got %d", stats.TotalCreated)
	}
}

// TestConnectionPool_MaxActive tests MaxActive limit
func TestConnectionPool_MaxActive(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         5,
		MaxActive:       3,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	// Get MaxActive connections
	conns := make([]net.Conn, config.MaxActive)
	for i := 0; i < config.MaxActive; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get connection %d: %v", i, err)
		}
		conns[i] = conn
	}

	// Check stats
	stats := pool.Stats()
	if stats.ActiveConnections != int32(config.MaxActive) {
		t.Errorf("Expected %d active connections, got %d", config.MaxActive, stats.ActiveConnections)
	}

	// Try to get one more connection with timeout - should fail
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := pool.Get(ctxWithTimeout)
	if err == nil {
		t.Error("Expected error when exceeding MaxActive, got nil")
	}

	// Return connections
	for _, conn := range conns {
		pool.Put(conn)
	}
}

// TestConnectionPool_IdleTimeout tests idle connection timeout
func TestConnectionPool_IdleTimeout(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     100 * time.Millisecond,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	// Get a connection
	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Return it to the pool
	pool.Put(conn)

	// Check that it's in the idle pool
	stats := pool.Stats()
	if stats.IdleConnections != 1 {
		t.Errorf("Expected 1 idle connection, got %d", stats.IdleConnections)
	}

	// Wait for idle timeout
	time.Sleep(200 * time.Millisecond)

	// The connection should still be there (cleaner runs periodically)
	// This is just a basic test - full idle timeout testing requires more complex setup
	stats = pool.Stats()
	t.Logf("Idle connections after timeout: %d", stats.IdleConnections)
}

// TestConnectionPool_Close tests pool close
func TestConnectionPool_Close(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)

	ctx := context.Background()

	// Get a connection
	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Close the pool
	pool.Close()

	// Try to get another connection - should fail
	_, err = pool.Get(ctx)
	if err == nil {
		t.Error("Expected error after pool close, got nil")
	}

	// Put should not panic
	pool.Put(conn)
}

// TestConnectionPool_ConcurrentAccess tests concurrent access to the pool
func TestConnectionPool_ConcurrentAccess(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       50,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	const numGoroutines = 100
	const opsPerGoroutine = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				conn, err := pool.Get(ctx)
				if err != nil {
					errors <- err
					return
				}
				pool.Put(conn)
			}
		}()
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		t.Errorf("Got %d errors during concurrent access", len(errors))
		for err := range errors {
			t.Logf("Error: %v", err)
		}
	}

	stats := pool.Stats()
	t.Logf("Final stats: active=%d, idle=%d, created=%d, checkedOut=%d",
		stats.ActiveConnections, stats.IdleConnections, stats.TotalCreated, stats.TotalCheckedOut)
}

// TestPoolManager_GetPool tests pool manager GetPool
func TestPoolManager_GetPool(t *testing.T) {
	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	manager := NewPoolManager(config)

	// Get pool for address
	pool1 := manager.GetPool("127.0.0.1:6379")
	if pool1 == nil {
		t.Fatal("Expected non-nil pool")
	}

	// Get same pool again - should return the same instance
	pool2 := manager.GetPool("127.0.0.1:6379")
	if pool1 != pool2 {
		t.Error("Expected same pool instance")
	}

	// Get pool for different address
	pool3 := manager.GetPool("127.0.0.1:6380")
	if pool1 == pool3 {
		t.Error("Expected different pool instance")
	}

	// Check pool count
	if manager.PoolCount() != 2 {
		t.Errorf("Expected 2 pools, got %d", manager.PoolCount())
	}

	// Clean up
	manager.CloseAll()
}

// TestPoolManager_RemovePool tests pool manager RemovePool
func TestPoolManager_RemovePool(t *testing.T) {
	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	manager := NewPoolManager(config)

	// Create a pool
	pool := manager.GetPool("127.0.0.1:6379")
	if pool == nil {
		t.Fatal("Expected non-nil pool")
	}

	// Remove the pool
	manager.RemovePool("127.0.0.1:6379")

	// Check pool count
	if manager.PoolCount() != 0 {
		t.Errorf("Expected 0 pools after removal, got %d", manager.PoolCount())
	}
}

// TestPoolManager_UpdatePools tests pool manager UpdatePools
func TestPoolManager_UpdatePools(t *testing.T) {
	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	manager := NewPoolManager(config)

	// Create pools for initial addresses
	addresses1 := []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
	for _, addr := range addresses1 {
		manager.GetPool(addr)
	}

	if manager.PoolCount() != 3 {
		t.Errorf("Expected 3 pools, got %d", manager.PoolCount())
	}

	// Update with fewer addresses
	addresses2 := []string{"127.0.0.1:6379", "127.0.0.1:6380"}
	manager.UpdatePools(addresses2)

	if manager.PoolCount() != 2 {
		t.Errorf("Expected 2 pools after update, got %d", manager.PoolCount())
	}

	// Clean up
	manager.CloseAll()
}

// BenchmarkConnectionPool_Get benchmarks Get operation
func BenchmarkConnectionPool_Get(b *testing.B) {
	listener := mockListener(b)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			b.Fatalf("Failed to get connection: %v", err)
		}
		pool.Put(conn)
	}
}

// BenchmarkConnectionPool_GetWithReuse benchmarks Get operation with connection reuse
func BenchmarkConnectionPool_GetWithReuse(b *testing.B) {
	listener := mockListener(b)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         100,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	// Pre-populate the pool with connections
	conns := make([]net.Conn, 10)
	for i := 0; i < 10; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			b.Fatalf("Failed to get connection: %v", err)
		}
		conns[i] = conn
	}

	// Return them to the pool
	for _, conn := range conns {
		pool.Put(conn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			b.Fatalf("Failed to get connection: %v", err)
		}
		pool.Put(conn)
	}
}

// BenchmarkConnectionPool_Concurrent benchmarks concurrent pool access
func BenchmarkConnectionPool_Concurrent(b *testing.B) {
	listener := mockListener(b)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         50,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get(ctx)
			if err != nil {
				b.Fatalf("Failed to get connection: %v", err)
			}
			pool.Put(conn)
		}
	})
}

// BenchmarkPoolManager_GetPool benchmarks pool manager GetPool
func BenchmarkPoolManager_GetPool(b *testing.B) {
	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	manager := NewPoolManager(config)
	defer manager.CloseAll()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetPool("127.0.0.1:6379")
	}
}

// TestConnectionPool_Stats tests pool statistics
func TestConnectionPool_Stats(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := PoolConfig{
		MaxIdle:         10,
		MaxActive:       100,
		IdleTimeout:     30 * time.Second,
		MaxConnLifetime: 5 * time.Minute,
	}

	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	ctx := context.Background()

	// Initial stats
	stats := pool.Stats()
	if stats.TotalCreated != 0 {
		t.Errorf("Expected 0 total created initially, got %d", stats.TotalCreated)
	}

	// Get a connection
	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	stats = pool.Stats()
	if stats.TotalCreated != 1 {
		t.Errorf("Expected 1 total created, got %d", stats.TotalCreated)
	}
	if stats.TotalCheckedOut != 1 {
		t.Errorf("Expected 1 total checked out, got %d", stats.TotalCheckedOut)
	}
	if stats.MissCount != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.MissCount)
	}

	// Return the connection
	pool.Put(conn)

	// Get it again - should be a hit
	conn2, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	pool.Put(conn2)

	stats = pool.Stats()
	if stats.HitCount != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.HitCount)
	}
}

// TestConnectionPool_Address tests Address method
func TestConnectionPool_Address(t *testing.T) {
	listener := mockListener(t)
	defer listener.Close()

	config := DefaultPoolConfig()
	pool := NewConnectionPool(listener.Addr().String(), config)
	defer pool.Close()

	if pool.Address() != listener.Addr().String() {
		t.Errorf("Expected address %s, got %s", listener.Addr().String(), pool.Address())
	}
}
