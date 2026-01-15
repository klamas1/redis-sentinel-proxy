package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	masterresolver "github.com/flant/redis-sentinel-proxy/pkg/master_resolver"
	"github.com/flant/redis-sentinel-proxy/pkg/proxy"
	replicaresolver "github.com/flant/redis-sentinel-proxy/pkg/replica_resolver"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		localAddr            = ":9999"
		replicaAddr          = ":9998"
		sentinelAddr         = ":26379"
		masterName           = "mymaster"
		masterResolveRetries = 3
		password             = ""
		balancingType        = "round-robin"
	)

	flag.StringVar(&localAddr, "listen", localAddr, "local address for master proxy")
	flag.StringVar(&replicaAddr, "replica-listen", replicaAddr, "local address for replica proxy")
	flag.StringVar(&sentinelAddr, "sentinel", sentinelAddr, "remote address")
	flag.StringVar(&masterName, "master", masterName, "name of the master redis node")
	flag.StringVar(&password, "password", password, "redis password")
	flag.IntVar(&masterResolveRetries, "resolve-retries", masterResolveRetries, "number of consecutive retries of the redis master node resolve")
	flag.StringVar(&balancingType, "balancing", balancingType, "balancing type for replicas: round-robin or leastconn")
	flag.Parse()

	if envPassword := os.Getenv("SENTINEL_PASSWORD"); envPassword != "" {
		password = envPassword
	}

	bt := parseBalancingType(balancingType)
	if err := runProxying(localAddr, replicaAddr, sentinelAddr, password, masterName, masterResolveRetries, bt); err != nil {
		log.Fatalf("Fatal: %s", err)
	}
	log.Println("Exiting...")
}

func parseBalancingType(s string) replicaresolver.BalancingType {
	switch s {
	case "leastconn":
		return replicaresolver.LeastConn
	default:
		return replicaresolver.RoundRobin
	}
}

func runProxying(localAddr, replicaAddr, sentinelAddr, password string, masterName string, masterResolveRetries int, bt replicaresolver.BalancingType) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	laddr := resolveTCPAddr(localAddr)
	raddr := resolveTCPAddr(replicaAddr)
	saddr := resolveTCPAddr(sentinelAddr)

	masterAddrResolver := masterresolver.NewRedisMasterResolver(masterName, saddr, password, masterResolveRetries)
	replicaAddrResolver := replicaresolver.NewReplicaResolver(masterName, saddr, password, masterResolveRetries, bt)

	masterProxy := proxy.NewRedisSentinelProxy(laddr, masterAddrResolver)
	replicaProxy := proxy.NewRedisSentinelProxy(raddr, replicaAddrResolver)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return masterAddrResolver.UpdateMasterAddressLoop(ctx) })
	eg.Go(func() error { return replicaAddrResolver.UpdateReplicasLoop(ctx) })
	eg.Go(func() error { return masterProxy.Run(ctx) })
	eg.Go(func() error { return replicaProxy.Run(ctx) })
	return eg.Wait()
}

func resolveTCPAddr(addr string) *net.TCPAddr {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("Fatal - Failed resolving tcp address: %s", err)
	}
	return tcpAddr
}
