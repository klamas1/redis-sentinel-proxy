package resolver

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klamas1/redis-sentinel-proxy/pkg/logger"
	"github.com/klamas1/redis-sentinel-proxy/pkg/utils"
)

// HealthConfig holds the configuration for HealthChecker
type HealthConfig struct {
	Interval            time.Duration // Interval between health checks
	Timeout             time.Duration // Timeout for each health check
	UnhealthyThreshold  int           // Number of consecutive failures to mark unhealthy
	HealthyThreshold    int           // Number of consecutive successes to mark healthy
}

// DefaultHealthConfig returns a sensible default configuration
func DefaultHealthConfig() HealthConfig {
	return HealthConfig{
		Interval:           5 * time.Second,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		HealthyThreshold:   2,
	}
}

// ReplicaHealth tracks the health status of a single replica
type ReplicaHealth struct {
	Address            string
	IsHealthy          int32 // atomic: 1 = healthy, 0 = unhealthy
	ConsecutiveSuccess int32
	ConsecutiveFailure int32
	LastCheckTime      int64 // Unix timestamp in nanoseconds
	LastError          string
}

// HealthChecker monitors the health of Redis replicas
type HealthChecker struct {
	config HealthConfig
	logger logger.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// Replica health tracking
	replicas     map[string]*ReplicaHealth
	replicasLock sync.RWMutex

	// Running state
	isRunning int32 // atomic: 1 = running, 0 = stopped
}

// NewHealthChecker creates a new HealthChecker with the given configuration
func NewHealthChecker(config HealthConfig, appLogger logger.Logger) *HealthChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &HealthChecker{
		config:   config,
		logger:   appLogger,
		ctx:      ctx,
		cancel:   cancel,
		replicas: make(map[string]*ReplicaHealth),
	}
}

// Start begins the health checking loop
func (hc *HealthChecker) Start(ctx context.Context) error {
	// Prevent multiple starts
	if !atomic.CompareAndSwapInt32(&hc.isRunning, 0, 1) {
		return nil // Already running
	}

	hc.logger.Debugf("[DEBUG] HealthChecker started with interval %v", hc.config.Interval)

	go hc.runHealthCheckLoop(ctx)
	return nil
}

// Stop stops the health checking loop
func (hc *HealthChecker) Stop() {
	if atomic.CompareAndSwapInt32(&hc.isRunning, 1, 0) {
		hc.cancel()
		hc.logger.Debug("[DEBUG] HealthChecker stopped")
	}
}

// UpdateReplicaList updates the list of replicas to monitor
func (hc *HealthChecker) UpdateReplicaList(replicas []*net.TCPAddr) {
	hc.replicasLock.Lock()
	defer hc.replicasLock.Unlock()

	// Create a new map with current replicas
	newReplicas := make(map[string]*ReplicaHealth, len(replicas))

	for _, replica := range replicas {
		addr := replica.String()
		if existing, ok := hc.replicas[addr]; ok {
			// Keep existing health data
			newReplicas[addr] = existing
		} else {
			// Create new health tracking for this replica
			newReplicas[addr] = &ReplicaHealth{
				Address:   addr,
				IsHealthy: 1, // Assume healthy until proven otherwise
			}
		}
	}

	// Log removed replicas
	for addr := range hc.replicas {
		if _, ok := newReplicas[addr]; !ok {
			hc.logger.Debugf("[DEBUG] HealthChecker: replica %s removed from monitoring", addr)
		}
	}

	hc.replicas = newReplicas

	hc.logger.Debugf("[DEBUG] HealthChecker: now monitoring %d replicas", len(hc.replicas))
}

// IsHealthy checks if a specific replica is currently healthy
func (hc *HealthChecker) IsHealthy(addr string) bool {
	hc.replicasLock.RLock()
	defer hc.replicasLock.RUnlock()

	health, ok := hc.replicas[addr]
	if !ok {
		return false
	}

	return atomic.LoadInt32(&health.IsHealthy) == 1
}

// GetHealthyReplicas returns a list of currently healthy replicas
func (hc *HealthChecker) GetHealthyReplicas() []*net.TCPAddr {
	hc.replicasLock.RLock()
	defer hc.replicasLock.RUnlock()

	var healthy []*net.TCPAddr
	for addr, health := range hc.replicas {
		if atomic.LoadInt32(&health.IsHealthy) == 1 {
			tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
			if err == nil {
				healthy = append(healthy, tcpAddr)
			}
		}
	}

	return healthy
}

// GetUnhealthyReplicas returns a list of currently unhealthy replicas
func (hc *HealthChecker) GetUnhealthyReplicas() []string {
	hc.replicasLock.RLock()
	defer hc.replicasLock.RUnlock()

	var unhealthy []string
	for addr, health := range hc.replicas {
		if atomic.LoadInt32(&health.IsHealthy) == 0 {
			unhealthy = append(unhealthy, addr)
		}
	}

	return unhealthy
}

// GetReplicaHealth returns the health status of a specific replica
func (hc *HealthChecker) GetReplicaHealth(addr string) *ReplicaHealth {
	hc.replicasLock.RLock()
	defer hc.replicasLock.RUnlock()

	return hc.replicas[addr]
}

// runHealthCheckLoop runs the health checking loop in the background
func (hc *HealthChecker) runHealthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(hc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.ctx.Done():
			return
		case <-ticker.C:
			hc.checkAllReplicas()
		}
	}
}

// checkAllReplicas performs health checks on all monitored replicas
func (hc *HealthChecker) checkAllReplicas() {
	hc.replicasLock.RLock()
	replicas := make([]*ReplicaHealth, 0, len(hc.replicas))
	for _, r := range hc.replicas {
		replicas = append(replicas, r)
	}
	hc.replicasLock.RUnlock()

	var wg sync.WaitGroup
	for _, replica := range replicas {
		wg.Add(1)
		go func(r *ReplicaHealth) {
			defer wg.Done()
			hc.checkReplica(r)
		}(replica)
	}
	wg.Wait()
}

// checkReplica performs a health check on a single replica
func (hc *HealthChecker) checkReplica(health *ReplicaHealth) {
	startTime := time.Now()

	// Perform TCP connection check
	conn, err := utils.TCPConnectWithTimeoutAndCustomTimeout(health.Address, hc.config.Timeout)
	checkDuration := time.Since(startTime)

	if err == nil {
		conn.Close()
		hc.recordSuccess(health, checkDuration)
	} else {
		hc.recordFailure(health, err.Error(), checkDuration)
	}
}

// recordSuccess records a successful health check
func (hc *HealthChecker) recordSuccess(health *ReplicaHealth, duration time.Duration) {
	consecutiveSuccess := atomic.AddInt32(&health.ConsecutiveSuccess, 1)
	atomic.StoreInt32(&health.ConsecutiveFailure, 0)
	atomic.StoreInt64(&health.LastCheckTime, time.Now().UnixNano())

	if consecutiveSuccess >= int32(hc.config.HealthyThreshold) {
		if atomic.CompareAndSwapInt32(&health.IsHealthy, 0, 1) {
			hc.logger.Debugf("[DEBUG] HealthChecker: replica %s is now healthy (after %d successes)",
				health.Address, consecutiveSuccess)
		}
	}

	hc.logger.Debugf("[DEBUG] HealthChecker: replica %s check OK (%v)", health.Address, duration)
}

// recordFailure records a failed health check
func (hc *HealthChecker) recordFailure(health *ReplicaHealth, errorMsg string, duration time.Duration) {
	consecutiveFailure := atomic.AddInt32(&health.ConsecutiveFailure, 1)
	atomic.StoreInt32(&health.ConsecutiveSuccess, 0)
	atomic.StoreInt64(&health.LastCheckTime, time.Now().UnixNano())
	health.LastError = errorMsg

	if consecutiveFailure >= int32(hc.config.UnhealthyThreshold) {
		if atomic.CompareAndSwapInt32(&health.IsHealthy, 1, 0) {
			hc.logger.Warn("[WARN] HealthChecker: replica %s is now unhealthy (after %d failures): %s",
				health.Address, consecutiveFailure, errorMsg)
		}
	}

	hc.logger.Debugf("[DEBUG] HealthChecker: replica %s check FAILED in %v: %s", health.Address, duration, errorMsg)
}

// IsRunning returns whether the health checker is currently running
func (hc *HealthChecker) IsRunning() bool {
	return atomic.LoadInt32(&hc.isRunning) == 1
}

// GetStats returns statistics about the health checker
func (hc *HealthChecker) GetStats() (total, healthy, unhealthy int) {
	hc.replicasLock.RLock()
	defer hc.replicasLock.RUnlock()

	total = len(hc.replicas)
	for _, health := range hc.replicas {
		if atomic.LoadInt32(&health.IsHealthy) == 1 {
			healthy++
		} else {
			unhealthy++
		}
	}
	return
}
