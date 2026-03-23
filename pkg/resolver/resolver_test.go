package resolver

import (
	"net"
	"sync"
	"testing"
)

// TestReplicaResolverIncrementConn tests the IncrementConn method
func TestReplicaResolverIncrementConn(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr := "127.0.0.1:6380"

	// Initial count should be 0
	if resolver.connCounts[addr] != 0 {
		t.Errorf("Initial conn count for %s = %d, want 0", addr, resolver.connCounts[addr])
	}

	// Increment connection count
	resolver.IncrementConn(addr)

	if resolver.connCounts[addr] != 1 {
		t.Errorf("After IncrementConn, count = %d, want 1", resolver.connCounts[addr])
	}

	// Increment again
	resolver.IncrementConn(addr)

	if resolver.connCounts[addr] != 2 {
		t.Errorf("After second IncrementConn, count = %d, want 2", resolver.connCounts[addr])
	}
}

// TestReplicaResolverDecrementConn tests the DecrementConn method
func TestReplicaResolverDecrementConn(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr := "127.0.0.1:6380"

	// Set initial count
	resolver.connCounts[addr] = 5

	// Decrement connection count
	resolver.DecrementConn(addr)

	if resolver.connCounts[addr] != 4 {
		t.Errorf("After DecrementConn, count = %d, want 4", resolver.connCounts[addr])
	}

	// Decrement to zero
	for i := 0; i < 4; i++ {
		resolver.DecrementConn(addr)
	}

	if resolver.connCounts[addr] != 0 {
		t.Errorf("After decrementing to zero, count = %d, want 0", resolver.connCounts[addr])
	}

	// Decrement below zero should not happen
	resolver.DecrementConn(addr)

	if resolver.connCounts[addr] != 0 {
		t.Errorf("After decrementing below zero, count = %d, want 0 (should not go negative)", resolver.connCounts[addr])
	}
}

// TestReplicaResolverIncrementDecrementConn tests the combination of IncrementConn and DecrementConn
func TestReplicaResolverIncrementDecrementConn(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr1 := "127.0.0.1:6380"
	addr2 := "127.0.0.1:6381"

	// Simulate multiple connections to different replicas
	resolver.IncrementConn(addr1)
	resolver.IncrementConn(addr1)
	resolver.IncrementConn(addr2)

	if resolver.connCounts[addr1] != 2 {
		t.Errorf("addr1 count = %d, want 2", resolver.connCounts[addr1])
	}
	if resolver.connCounts[addr2] != 1 {
		t.Errorf("addr2 count = %d, want 1", resolver.connCounts[addr2])
	}

	// Simulate connection closures
	resolver.DecrementConn(addr1)
	resolver.DecrementConn(addr2)
	resolver.DecrementConn(addr2) // This should not go negative

	if resolver.connCounts[addr1] != 1 {
		t.Errorf("After decrement, addr1 count = %d, want 1", resolver.connCounts[addr1])
	}
	if resolver.connCounts[addr2] != 0 {
		t.Errorf("After decrement, addr2 count = %d, want 0", resolver.connCounts[addr2])
	}
}

// TestReplicaResolverIncrementConnConcurrent tests concurrent IncrementConn calls
func TestReplicaResolverIncrementConnConcurrent(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr := "127.0.0.1:6380"
	numGoroutines := 100
	var wg sync.WaitGroup

	// Launch multiple goroutines that increment the counter
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resolver.IncrementConn(addr)
		}()
	}

	wg.Wait()

	if resolver.connCounts[addr] != numGoroutines {
		t.Errorf("After %d concurrent increments, count = %d, want %d", numGoroutines, resolver.connCounts[addr], numGoroutines)
	}
}

// TestReplicaResolverDecrementConnConcurrent tests concurrent DecrementConn calls
func TestReplicaResolverDecrementConnConcurrent(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr := "127.0.0.1:6380"
	numGoroutines := 100

	// Set initial count
	resolver.connCounts[addr] = numGoroutines

	var wg sync.WaitGroup

	// Launch multiple goroutines that decrement the counter
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resolver.DecrementConn(addr)
		}()
	}

	wg.Wait()

	if resolver.connCounts[addr] != 0 {
		t.Errorf("After %d concurrent decrements, count = %d, want 0", numGoroutines, resolver.connCounts[addr])
	}
}

// TestReplicaResolverIncrementDecrementConnConcurrent tests concurrent IncrementConn and DecrementConn calls
func TestReplicaResolverIncrementDecrementConnConcurrent(t *testing.T) {
	resolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	addr := "127.0.0.1:6380"
	numGoroutines := 100
	var wg sync.WaitGroup

	// Launch goroutines that increment and decrement
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resolver.IncrementConn(addr)
			resolver.DecrementConn(addr)
		}()
	}

	wg.Wait()

	// Final count should be 0 since each increment is followed by a decrement
	if resolver.connCounts[addr] != 0 {
		t.Errorf("After %d concurrent increment/decrement pairs, count = %d, want 0", numGoroutines, resolver.connCounts[addr])
	}
}

// TestRedisMasterResolverIncrementConn tests that IncrementConn is a no-op for master
func TestRedisMasterResolverIncrementConn(t *testing.T) {
	resolver := &RedisMasterResolver{}

	// This should not panic or do anything
	resolver.IncrementConn("127.0.0.1:6379")

	// No state to check since it's a no-op
	t.Log("IncrementConn on RedisMasterResolver completed without error (no-op)")
}

// TestRedisSentinelResolverIncrementConn tests IncrementConn delegation
func TestRedisSentinelResolverIncrementConn(t *testing.T) {
	masterResolver := &RedisMasterResolver{}
	replicaResolver := &ReplicaResolver{
		connCounts:   make(map[string]int),
		connCountsLock: &sync.Mutex{},
	}

	resolver := &RedisSentinelResolver{
		masterResolver:  masterResolver,
		replicaResolver: replicaResolver,
	}

	addr := "127.0.0.1:6380"

	// Increment connection count
	resolver.IncrementConn(addr)

	// Check that replica resolver's count was incremented
	if replicaResolver.connCounts[addr] != 1 {
		t.Errorf("After IncrementConn, replica count = %d, want 1", replicaResolver.connCounts[addr])
	}

	// Decrement connection count
	resolver.DecrementConn(addr)

	if replicaResolver.connCounts[addr] != 0 {
		t.Errorf("After DecrementConn, replica count = %d, want 0", replicaResolver.connCounts[addr])
	}
}

// TestReplicaResolverLeastConnWithIncrementDecrement tests LeastConn balancing with proper increment/decrement
func TestReplicaResolverLeastConnWithIncrementDecrement(t *testing.T) {
	// Create resolver with LeastConn balancing
	sentinelAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:26379")
	resolver := &ReplicaResolver{
		sentinelAddr:       sentinelAddr,
		balancingType:      LeastConn,
		connCounts:         make(map[string]int),
		connCountsLock:     &sync.Mutex{},
		replicasLock:       &sync.RWMutex{},
		initialResolveLock: make(chan struct{}),
	}
	close(resolver.initialResolveLock)

	// Create test replicas
	replica1, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:6380")
	replica2, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:6381")
	resolver.replicas = []*net.TCPAddr{replica1, replica2}

	// Simulate connections to replica1
	resolver.IncrementConn(replica1.String())
	resolver.IncrementConn(replica1.String())

	// Now replica1 has 2 connections, replica2 has 0
	// LeastConn should select replica2
	selectedAddr := resolver.Address()
	if selectedAddr != replica2.String() {
		t.Errorf("LeastConn selected %s, want %s (least connections)", selectedAddr, replica2.String())
	}

	// After selection, we should increment (simulating successful connection)
	// But Address() doesn't increment anymore - that's done by the caller
	// So we manually increment to simulate
	resolver.IncrementConn(selectedAddr)

	// Now both have 1 connection each (replica1: 2, replica2: 1)
	// Decrement replica1 to simulate connection close
	resolver.DecrementConn(replica1.String())
	resolver.DecrementConn(replica1.String())

	// Now replica1 has 0, replica2 has 1
	// LeastConn should select replica1
	selectedAddr = resolver.Address()
	if selectedAddr != replica1.String() {
		t.Errorf("LeastConn selected %s, want %s (least connections)", selectedAddr, replica1.String())
	}
}
