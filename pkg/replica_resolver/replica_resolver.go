package replicaresolver

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	masterresolver "github.com/flant/redis-sentinel-proxy/pkg/master_resolver"
)

type BalancingType int

const (
	RoundRobin BalancingType = iota
	LeastConn
)

type ReplicaResolver struct {
	masterName               string
	sentinelAddr             *net.TCPAddr
	sentinelPassword         string
	retryOnResolveFail       int
	balancingType            BalancingType

	replicasLock             *sync.RWMutex
	initialResolveLock       chan struct{}

	replicas                 []*net.TCPAddr
	currentIndex             int
	connCounts               map[string]int
	connCountsLock           *sync.Mutex
}

func NewReplicaResolver(masterName string, sentinelAddr *net.TCPAddr, sentinelPassword string, retryOnResolveFail int, balancingType BalancingType) *ReplicaResolver {
	return &ReplicaResolver{
		masterName:               masterName,
		sentinelAddr:             sentinelAddr,
		sentinelPassword:         sentinelPassword,
		retryOnResolveFail:       retryOnResolveFail,
		balancingType:            balancingType,
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

	switch r.balancingType {
	case RoundRobin:
		addr := r.replicas[r.currentIndex%len(r.replicas)]
		r.currentIndex++
		return addr.String()
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
		}
		return selected.String()
	default:
		return r.replicas[0].String()
	}
}

func (r *ReplicaResolver) DecrementConn(addr string) {
	r.connCountsLock.Lock()
	defer r.connCountsLock.Unlock()
	if r.connCounts[addr] > 0 {
		r.connCounts[addr]--
	}
}

func (r *ReplicaResolver) UpdateReplicasLoop(ctx context.Context) error {
	if err := r.initialReplicaResolve(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var err error
	for errCount := 0; errCount <= r.retryOnResolveFail; {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		err = r.updateReplicas()
		if err != nil {
			errCount++
		} else {
			errCount = 0
		}
	}
	return err
}

func (r *ReplicaResolver) initialReplicaResolve() error {
	defer close(r.initialResolveLock)
	return r.updateReplicas()
}

func (r *ReplicaResolver) updateReplicas() error {
	replicas, err := masterresolver.RedisReplicasFromSentinelAddr(r.sentinelAddr, r.sentinelPassword, r.masterName)
	if err != nil {
		log.Println(err)
		return err
	}
	r.setReplicas(replicas)
	return nil
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