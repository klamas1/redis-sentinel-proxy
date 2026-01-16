package resolver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

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

	masterAddrLock           *sync.RWMutex
	initialMasterResolveLock chan struct{}

	masterAddr     string
	previousMaster string
}

func NewRedisMasterResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnMasterResolveFail int) *RedisMasterResolver {
	return &RedisMasterResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnMasterResolveFail: retryOnMasterResolveFail,
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

func (r *RedisMasterResolver) RetryOnResolveFail() int {
	return r.retryOnMasterResolveFail
}

func (r *RedisMasterResolver) setMasterAddress(masterAddr *net.TCPAddr) {
	r.masterAddrLock.Lock()
	defer r.masterAddrLock.Unlock()
	newAddr := masterAddr.String()
	if r.masterAddr != "" && r.masterAddr != newAddr {
		log.Printf("[DEBUG] Master switched from %s to %s", r.masterAddr, newAddr)
	}
	r.previousMaster = r.masterAddr
	r.masterAddr = newAddr
}

func (r *RedisMasterResolver) UpdateMasterAddress() error {
	masterAddr, err := redisMasterFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName)
	if err != nil {
		log.Println(err)
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
	debug                    bool

	replicasLock             *sync.RWMutex
	initialResolveLock       chan struct{}

	replicas                 []*net.TCPAddr
	currentIndex             int
	connCounts               map[string]int
	connCountsLock           *sync.Mutex
}

func NewReplicaResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnResolveFail int, balancingType BalancingType, debug bool) *ReplicaResolver {
	return &ReplicaResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnResolveFail:       retryOnResolveFail,
		balancingType:            balancingType,
		debug:                    debug,
		replicasLock:             &sync.RWMutex{},
		initialResolveLock:       make(chan struct{}),
		connCounts:               make(map[string]int),
		connCountsLock:           &sync.Mutex{},
	}
}

func (r *ReplicaResolver) Address() string {
	<-r.initialResolveLock

	r.replicasLock.RLock()
	defer r.replicasLock.RUnlock()

	if len(r.replicas) == 0 {
		return ""
	}

	var selectedAddr string
	switch r.balancingType {
	case RoundRobin:
		addr := r.replicas[r.currentIndex%len(r.replicas)]
		r.currentIndex++
		selectedAddr = addr.String()
	case LeastConn:
		r.connCountsLock.Lock()
		defer r.connCountsLock.Unlock()
		minCount := -1
		var selected *net.TCPAddr
		for _, replica := range r.replicas {
			key := replica.String()
			count := r.connCounts[key]
			if minCount == -1 || count < minCount {
				minCount = count
				selected = replica
			}
		}
		if selected != nil {
			r.connCounts[selected.String()]++
			selectedAddr = selected.String()
		}
	default:
		selectedAddr = r.replicas[0].String()
	}

	if r.debug {
		log.Printf("Selected replica %s at %s", selectedAddr, time.Now().Format("2006-01-02 15:04:05"))
	}
	return selectedAddr
}

func (r *ReplicaResolver) DecrementConn(addr string) {
	r.connCountsLock.Lock()
	defer r.connCountsLock.Unlock()
	if r.connCounts[addr] > 0 {
		r.connCounts[addr]--
	}
}

func (r *ReplicaResolver) RetryOnResolveFail() int {
	return r.retryOnResolveFail
}


func (r *ReplicaResolver) UpdateReplicas(masterAddr string) error {
	replicas, err := RedisReplicasFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName)
	if err != nil {
		log.Println(err)
		return err
	}
	// Filter out the current master
	var filtered []*net.TCPAddr
	for _, replica := range replicas {
		if replica.String() != masterAddr {
			filtered = append(filtered, replica)
		}
	}
	r.setReplicas(filtered)
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
	// Reset currentIndex if needed
	if r.currentIndex >= len(replicas) {
		r.currentIndex = 0
	}
}

type RedisSentinelResolver struct {
	masterResolver  *RedisMasterResolver
	replicaResolver *ReplicaResolver
	debug           bool
}

func NewRedisSentinelResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnResolveFail int, balancingType BalancingType, debug bool) *RedisSentinelResolver {
	masterResolver := NewRedisMasterResolver(masterName, sentinelAddr, sentinelPassword, retryOnResolveFail)
	replicaResolver := NewReplicaResolver(masterName, sentinelAddr, sentinelPassword, retryOnResolveFail, balancingType, debug)
	return &RedisSentinelResolver{
		masterResolver:  masterResolver,
		replicaResolver: replicaResolver,
		debug:           debug,
	}
}

func (r *RedisSentinelResolver) MasterAddress() string {
	addr := r.masterResolver.Address()
	if r.debug {
		log.Printf("[DEBUG] Resolver request: master address -> %s", addr)
	}
	return addr
}

func (r *RedisSentinelResolver) ReplicaAddress() string {
	addr := r.replicaResolver.Address()
	if r.debug && addr != "" {
		log.Printf("[DEBUG] Resolver request: replica address -> %s", addr)
	}
	return addr
}

func (r *RedisSentinelResolver) DecrementConn(addr string) {
	r.replicaResolver.DecrementConn(addr)
}

func (r *RedisSentinelResolver) UpdateLoop(ctx context.Context) error {
	if err := r.initialResolve(); err != nil {
		return err
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
			log.Println("Master update error:", masterErr)
			continue
		}

		// Then update replicas, excluding the current master
		masterAddr := r.masterResolver.Address()
		replicaErr := r.replicaResolver.UpdateReplicas(masterAddr)

		if replicaErr != nil {
			errCount++
			err = replicaErr
			log.Println("Replica update error:", replicaErr)
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
	masterAddr := r.masterResolver.Address()
	if err := r.replicaResolver.UpdateReplicas(masterAddr); err != nil {
		return err
	}
	if r.debug {
		log.Printf("[DEBUG] Initial setup: master %s, replicas %v", masterAddr, r.replicaResolver.replicas)
	}
	return nil
}

func redisMasterFromSentinelAddr(sentinelAddress *net.TCPAddr, sentinelPassword string, masterName string) (*net.TCPAddr, error) {
	conn, err := utils.TCPConnectWithTimeout(sentinelAddress.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to sentinel: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(time.Second))

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

func RedisReplicasFromSentinelAddr(sentinelAddress *net.TCPAddr, sentinelPassword string, masterName string) ([]*net.TCPAddr, error) {
	conn, err := utils.TCPConnectWithTimeout(sentinelAddress.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to sentinel: %w", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(time.Second))

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

	// Request replicas
	getReplicasCommand := fmt.Sprintf("SENTINEL REPLICAS %s\r\n", masterName)
	if _, err := conn.Write([]byte(getReplicasCommand)); err != nil {
		return nil, fmt.Errorf("error writing to sentinel: %w", err)
	}

	// Read response
	var response strings.Builder
	b := make([]byte, 2048)
	for {
		n, err := conn.Read(b)
		if err != nil {
			break
		}
		response.Write(b[:n])
		if n < len(b) {
			break
		}
	}

	log.Printf("[DEBUG] Sentinel slaves response: %q", response.String())

	// Разбиваем на части и фильтруем пустые строки
	rawParts := strings.Split(response.String(), "\r\n")
	var parts []string
	for _, part := range rawParts {
		if part != "" {
			parts = append(parts, part)
		}
	}

	log.Printf("[DEBUG] Parts: %v", parts)
	if len(parts) < 1 {
		return nil, errors.New("couldn't get replicas from sentinel")
	}

	// Parse RESP array
	if !strings.HasPrefix(parts[0], "*") {
		return nil, errors.New("invalid response format")
	}
	numSlaves, err := strconv.Atoi(parts[0][1:])
	if err != nil {
		return nil, fmt.Errorf("error parsing number of slaves: %w", err)
	} else {
    log.Printf("[DEBUG] numSlaves=%d", numSlaves)
  }

	var replicas []*net.TCPAddr
	index := 1
	for i := 0; i < numSlaves; i++ {
		log.Printf("[DEBUG] i=%d, index=%d, len(parts)=%d", i, index, len(parts))
		if index >= len(parts) || !strings.HasPrefix(parts[index], "*") {
			log.Printf("[DEBUG] Breaking loop at i=%d, index=%d, len(parts)=%d", i, index, len(parts))
			break
		}
		numFields, err := strconv.Atoi(parts[index][1:])
		if err != nil {
			return nil, fmt.Errorf("error parsing number of fields for slave %d: %w", i, err)
		}
		log.Printf("[DEBUG] numFields=%d", numFields)
		index++
		var ip, port string
		for j := 0; j < numFields; j++ {
			log.Printf("[DEBUG] j=%d, index=%d", j, index)
			
			// Проверка на достаточность элементов для парсинга
			if index+1 >= len(parts) || !strings.HasPrefix(parts[index], "$") {
				log.Printf("[DEBUG] Breaking field loop at j=%d, index=%d, len(parts)=%d", j, index, len(parts))
				log.Printf("[DEBUG] Remaining parts: %v", parts[index:])
				break
			}
			// Skip $len for key
			index++
			key := parts[index]
			index++
			
			if index+1 >= len(parts) || !strings.HasPrefix(parts[index], "$") {
				log.Printf("[DEBUG] Breaking field loop at j=%d, index=%d, len(parts)=%d", j, index, len(parts))
				log.Printf("[DEBUG] Remaining parts: %v", parts[index:])
				break
			}
			// Skip $len for value
			index++
			value := parts[index]
			index++
			
			if key == "ip" {
				ip = value
			}
			if key == "port" {
				port = value
			}
		}
		if ip != "" && port != "" {
			log.Printf("[DEBUG] Found replica %s:%s", ip, port)
			addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", ip, port))
			if err != nil {
				log.Printf("error resolving replica address %s:%s: %v", ip, port, err)
				continue
			}
			// Check if replica is accessible
			if err := checkTCPConnect(addr); err != nil {
				log.Printf("[DEBUG] Replica %s failed: %v", addr.String(), err)
				continue
			}
			log.Printf("[DEBUG] Replica %s accessible", addr.String())
			replicas = append(replicas, addr)
		}
	}

	log.Printf("[DEBUG] Total replicas found: %d", len(replicas))
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