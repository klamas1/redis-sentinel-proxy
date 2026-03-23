package resolver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klamas1/redis-sentinel-proxy/pkg/logger"
	"github.com/klamas1/redis-sentinel-proxy/pkg/metrics"
	"github.com/klamas1/redis-sentinel-proxy/pkg/resp"
	"github.com/klamas1/redis-sentinel-proxy/pkg/utils"
)

type BalancingType int

const (
	RoundRobin BalancingType = iota
	LeastConn
)

type RedisMasterResolver struct {
	masterName               string
	sentinelAddr             *net.TCPAddr
	sentinelPassword         string
	retryOnMasterResolveFail int
	logger                   logger.Logger

	masterAddrLock           *sync.RWMutex
	initialMasterResolveLock chan struct{}

	masterAddr     string
	previousMaster string
}

func NewRedisMasterResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnMasterResolveFail int, appLogger logger.Logger) *RedisMasterResolver {
	return &RedisMasterResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnMasterResolveFail: retryOnMasterResolveFail,
		logger:                   appLogger,
		masterAddrLock:           &sync.RWMutex{},
		initialMasterResolveLock: make(chan struct{}),
	}
}

func (r *RedisMasterResolver) Address() string {
	<-r.initialMasterResolveLock

	r.masterAddrLock.RLock()
	defer r.masterAddrLock.RUnlock()
	return r.masterAddr
}

func (r *RedisMasterResolver) DecrementConn(addr string) {
	// No-op for master
}

// IncrementConn is a no-op for master resolver
func (r *RedisMasterResolver) IncrementConn(addr string) {
	// No-op for master
}

func (r *RedisMasterResolver) RetryOnResolveFail() int {
	return r.retryOnMasterResolveFail
}

func (r *RedisMasterResolver) setMasterAddress(masterAddr *net.TCPAddr) {
	r.masterAddrLock.Lock()
	defer r.masterAddrLock.Unlock()
	newAddr := masterAddr.String()
	if r.masterAddr != "" && r.masterAddr != newAddr {
		r.logger.Info("Master switched from %s to %s", r.masterAddr, newAddr)
	}
	r.previousMaster = r.masterAddr
	r.masterAddr = newAddr
}

func (r *RedisMasterResolver) UpdateMasterAddress() error {
	masterAddr, err := redisMasterFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName, r.logger)
	if err != nil {
		r.logger.Error("Failed to update master address: %s", err)
		return err
	}
	r.setMasterAddress(masterAddr)
	return nil
}

func (r *RedisMasterResolver) UpdateMasterAddressLoop(ctx context.Context) error {
	if err := r.InitialMasterAddressResolve(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var err error
	for errCount := 0; errCount <= r.retryOnMasterResolveFail; {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		err = r.UpdateMasterAddress()
		if err != nil {
			errCount++
		} else {
			errCount = 0
		}
	}
	return err
}

func (r *RedisMasterResolver) InitialMasterAddressResolve() error {
	defer close(r.initialMasterResolveLock)
	return r.UpdateMasterAddress()
}

type ReplicaResolver struct {
	masterName               string
	sentinelAddr             *net.TCPAddr
	sentinelPassword         string
	retryOnResolveFail       int
	balancingType            BalancingType
	logger                   logger.Logger
	metrics                  *metrics.Metrics

	replicasLock             *sync.RWMutex
	initialResolveLock       chan struct{}

	replicas                 []*net.TCPAddr
	currentIndex             atomic.Int32
	connCounts               map[string]int
	connCountsLock           *sync.Mutex

	// Health checker integration
	healthChecker *HealthChecker
}

func NewReplicaResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnResolveFail int, balancingType BalancingType, appLogger logger.Logger, healthChecker *HealthChecker, appMetrics *metrics.Metrics) *ReplicaResolver {
	return &ReplicaResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnResolveFail:       retryOnResolveFail,
		balancingType:            balancingType,
		logger:                   appLogger,
		metrics:                  appMetrics,
		replicasLock:             &sync.RWMutex{},
		initialResolveLock:       make(chan struct{}),
		connCounts:               make(map[string]int),
		connCountsLock:           &sync.Mutex{},
		healthChecker:            healthChecker,
	}
}

func (r *ReplicaResolver) Address() string {
	<-r.initialResolveLock

	// Get healthy replicas if health checker is enabled
	var replicasToUse []*net.TCPAddr
	if r.healthChecker != nil {
		replicasToUse = r.healthChecker.GetHealthyReplicas()
	} else {
		r.replicasLock.RLock()
		replicasToUse = append([]*net.TCPAddr(nil), r.replicas...)
		r.replicasLock.RUnlock()
	}

	if len(replicasToUse) == 0 {
		r.logger.Error("[ERROR] No healthy replicas available for balancing")
		return ""
	}

	var selectedAddr string
	switch r.balancingType {
	case RoundRobin:
		// Use atomic operations to avoid race condition
		idx := r.currentIndex.Add(1) - 1
		addr := replicasToUse[idx%int32(len(replicasToUse))]
		selectedAddr = addr.String()
	case LeastConn:
		r.connCountsLock.Lock()
		minCount := -1
		var selected *net.TCPAddr
		for _, replica := range replicasToUse {
			key := replica.String()
			count := r.connCounts[key]
			if minCount == -1 || count < minCount {
				minCount = count
				selected = replica
			}
		}
		if selected != nil {
			selectedAddr = selected.String()
		}
		r.connCountsLock.Unlock()
	default:
		selectedAddr = replicasToUse[0].String()
	}

	r.logger.Info("Selected replica %s", selectedAddr)
	return selectedAddr
}

func (r *ReplicaResolver) DecrementConn(addr string) {
	r.connCountsLock.Lock()
	defer r.connCountsLock.Unlock()
	if r.connCounts[addr] > 0 {
		r.connCounts[addr]--
	}
}

// IncrementConn increments the connection count for a specific replica address
// This is called after a successful connection to ensure accurate load balancing
func (r *ReplicaResolver) IncrementConn(addr string) {
	r.connCountsLock.Lock()
	defer r.connCountsLock.Unlock()
	r.connCounts[addr]++
}

func (r *ReplicaResolver) RetryOnResolveFail() int {
	return r.retryOnResolveFail
}

func (r *ReplicaResolver) UpdateReplicas() error {
	replicas, err := RedisReplicasFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName, r.logger)
	if err != nil {
		r.logger.Error("Failed to update replicas: %s", err)
		return err
	}
	r.logger.Debugf("[DEBUG] Sentinel slaves response: %v", replicas)
	r.setReplicas(replicas)
	r.logger.Info("Set %d replicas", len(replicas))
	return nil
}

func (r *ReplicaResolver) GetReplicas() []*net.TCPAddr {
	r.replicasLock.RLock()
	defer r.replicasLock.RUnlock()
	return append([]*net.TCPAddr(nil), r.replicas...) // copy
}

func (r *ReplicaResolver) setReplicas(replicas []*net.TCPAddr) {
	r.replicasLock.Lock()
	defer r.replicasLock.Unlock()
	r.replicas = replicas
	// Reset currentIndex if needed using atomic operations
	current := r.currentIndex.Load()
	if current >= int32(len(replicas)) {
		r.currentIndex.Store(0)
	}
	// Update health checker with new replica list
	if r.healthChecker != nil {
		r.healthChecker.UpdateReplicaList(replicas)
	}
	// Update metrics for each replica
	if r.metrics != nil {
		for _, replica := range replicas {
			addr := replica.String()
			if r.healthChecker != nil {
				healthy := r.healthChecker.IsHealthy(addr)
				r.metrics.SetReplicaHealth(addr, healthy)
			} else {
				r.metrics.SetReplicaHealth(addr, true)
			}
		}
	}
}

type RedisSentinelResolver struct {
	masterResolver  *RedisMasterResolver
	replicaResolver *ReplicaResolver
	healthChecker   *HealthChecker
	logger          logger.Logger
	metrics         *metrics.Metrics
}

func NewRedisSentinelResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnResolveFail int, balancingType BalancingType, appLogger logger.Logger, healthConfig *HealthConfig, appMetrics *metrics.Metrics) *RedisSentinelResolver {
	// Create health checker if config is provided
	var healthChecker *HealthChecker
	if healthConfig != nil {
		healthChecker = NewHealthChecker(*healthConfig, appLogger)
	}

	masterResolver := NewRedisMasterResolver(masterName, sentinelAddr, sentinelPassword, retryOnResolveFail, appLogger)
	replicaResolver := NewReplicaResolver(masterName, sentinelAddr, sentinelPassword, retryOnResolveFail, balancingType, appLogger, healthChecker, appMetrics)
	return &RedisSentinelResolver{
		masterResolver:  masterResolver,
		replicaResolver: replicaResolver,
		healthChecker:   healthChecker,
		logger:          appLogger,
		metrics:         appMetrics,
	}
}

func (r *RedisSentinelResolver) MasterAddress() string {
	addr := r.masterResolver.Address()
	r.logger.Debugf("[DEBUG] Resolver request: master address -> %s", addr)
	return addr
}

func (r *RedisSentinelResolver) ReplicaAddress() string {
	addr := r.replicaResolver.Address()
	if addr != "" {
		r.logger.Debugf("[DEBUG] Resolver request: replica address -> %s", addr)
	}
	return addr
}

func (r *RedisSentinelResolver) DecrementConn(addr string) {
	r.replicaResolver.DecrementConn(addr)
}

func (r *RedisSentinelResolver) IncrementConn(addr string) {
	r.replicaResolver.IncrementConn(addr)
}

func (r *RedisSentinelResolver) UpdateLoop(ctx context.Context) error {
	if err := r.initialResolve(); err != nil {
		return err
	}

	// Start health checker if enabled
	if r.healthChecker != nil {
		r.healthChecker.Start(ctx)
		defer r.healthChecker.Stop()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var err error
	retryCount := r.masterResolver.RetryOnResolveFail()
	for errCount := 0; errCount <= retryCount; {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		// Update master first
		masterErr := r.masterResolver.UpdateMasterAddress()
		if masterErr != nil {
			errCount++
			err = masterErr
			r.logger.Error("Master update error: %s", masterErr)
			continue
		}

		// Then update replicas
		replicaErr := r.replicaResolver.UpdateReplicas()

		if replicaErr != nil {
			errCount++
			err = replicaErr
			r.logger.Error("Replica update error: %s", replicaErr)
		} else {
			errCount = 0
		}
	}
	return err
}

func (r *RedisSentinelResolver) initialResolve() error {
	if err := r.masterResolver.InitialMasterAddressResolve(); err != nil {
		return err
	}
	if err := r.replicaResolver.UpdateReplicas(); err != nil {
		return err
	}
	r.logger.Debugf("[DEBUG] Initial setup: master %s, replicas %v", r.masterResolver.Address(), r.replicaResolver.replicas)
	return nil
}

func redisMasterFromSentinelAddr(sentinelAddress *net.TCPAddr, sentinelPassword string, masterName string, appLogger logger.Logger) (*net.TCPAddr, error) {
	conn, err := utils.TCPConnectWithTimeout(sentinelAddress.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to sentinel: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Authenticate with sentinel if password is provided
	if sentinelPassword != "" {
		authCommand := fmt.Sprintf("AUTH %s\r\n", sentinelPassword)
		if _, err := conn.Write([]byte(authCommand)); err != nil {
			return nil, fmt.Errorf("error sending AUTH to sentinel: %w", err)
		}

		// Read response from AUTH
		b := make([]byte, 256)
		n, err := conn.Read(b)
		if err != nil {
			return nil, fmt.Errorf("error reading AUTH response: %w", err)
		}
		response := string(b[:n])
		if !strings.HasPrefix(response, "+OK") {
			return nil, fmt.Errorf("sentinel AUTH failed: %s", response)
		}
	}

	// Request master address
	getMasterCommand := fmt.Sprintf("SENTINEL get-master-addr-by-name %s\r\n", masterName)
	if _, err := conn.Write([]byte(getMasterCommand)); err != nil {
		return nil, fmt.Errorf("error writing to sentinel: %w", err)
	}

	// Read response
	b := make([]byte, 256)
	n, err := conn.Read(b)
	if err != nil {
		return nil, fmt.Errorf("error getting info from sentinel: %w", err)
	}
	// Extract master address parts
	parts := strings.Split(string(b[:n]), "\r\n")
	if len(parts) < 5 {
		return nil, errors.New("couldn't get master address from sentinel")
	}

	appLogger.Debugf("[DEBUG] Received response from sentinel: %s", parts)

	// Assemble master address
	formattedMasterAddress := fmt.Sprintf("%s:%s", parts[2], parts[4])
	addr, err := net.ResolveTCPAddr("tcp", formattedMasterAddress)
	if err != nil {
		return nil, fmt.Errorf("error resolving redis master: %w", err)
	}

	// Check if there is a Redis instance listening on the master address
	if err := checkTCPConnect(addr); err != nil {
		return nil, fmt.Errorf("error checking redis master: %w", err)
	}

	return addr, nil
}

func RedisReplicasFromSentinelAddr(sentinelAddress *net.TCPAddr, sentinelPassword string, masterName string, appLogger logger.Logger) ([]*net.TCPAddr, error) {
	conn, err := utils.TCPConnectWithTimeout(sentinelAddress.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to sentinel: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Authenticate with sentinel if password is provided
	if sentinelPassword != "" {
		authCommand := fmt.Sprintf("AUTH %s\r\n", sentinelPassword)
		if _, err := conn.Write([]byte(authCommand)); err != nil {
			return nil, fmt.Errorf("error sending AUTH to sentinel: %w", err)
		}

		// Read response from AUTH
		b := make([]byte, 256)
		n, err := conn.Read(b)
		if err != nil {
			return nil, fmt.Errorf("error reading AUTH response: %w", err)
		}
		response := string(b[:n])
		if !strings.HasPrefix(response, "+OK") {
			return nil, fmt.Errorf("sentinel AUTH failed: %s", response)
		}
	}

	// Request replica address
	getReplicasCommand := fmt.Sprintf("SENTINEL REPLICAS %s\r\n", masterName)
	if _, err := conn.Write([]byte(getReplicasCommand)); err != nil {
		return nil, fmt.Errorf("error writing to sentinel: %w", err)
	}

	// Read response
	var response strings.Builder
	b := make([]byte, 8192)
	for {
		n, err := conn.Read(b)
		if err != nil {
			if err.Error() == "EOF" || strings.Contains(err.Error(), "timeout") {
				break
			}
			return nil, fmt.Errorf("error reading from sentinel: %w", err)
		}
		response.Write(b[:n])
		if n < len(b) {
			break
		}
	}
	responseStr := response.String()

	appLogger.Debugf("[DEBUG] Sentinel slaves response: %q", responseStr)

	// Split into parts and filter out empty strings
	rawParts := strings.Split(responseStr, "\r\n")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		if part != "" {
			parts = append(parts, part)
		}
	}

	if len(parts) < 1 {
		return nil, errors.New("couldn't get replicas from sentinel")
	}

	// Use the new RESP parser
	parser := resp.NewRespParserFromParts(parts, appLogger)
	replicaMaps, err := parser.ParseSentinelReplicas()
	if err != nil {
		return nil, fmt.Errorf("error parsing sentinel replicas: %w", err)
	}

	var replicas []*net.TCPAddr
	for _, rep := range replicaMaps {
		// Assemble replica address
		formattedReplicaAddress := fmt.Sprintf("%s:%s", rep["ip"], rep["port"])
		addr, err := net.ResolveTCPAddr("tcp", formattedReplicaAddress)
		if err != nil {
			return nil, fmt.Errorf("Error resolving replica address %s: %v", formattedReplicaAddress, err)
		}
		// Check if replica is accessible
		// if err := checkTCPConnect(addr); err != nil {
		//   return nil, fmt.Errorf("Replica %s failed: %v", addr.String(), err)
		//   continue
		// }
		appLogger.Debugf("[DEBUG] Replica address %s accessible", addr.String())
		replicas = append(replicas, addr)
	}

	appLogger.Info("Total replicas found: %d", len(replicas))
	return replicas, nil
}

func checkTCPConnect(addr *net.TCPAddr) error {
	conn, err := utils.TCPConnectWithTimeout(addr.String())
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}
