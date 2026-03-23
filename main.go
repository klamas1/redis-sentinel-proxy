package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/klamas1/redis-sentinel-proxy/pkg/logger"
	"github.com/klamas1/redis-sentinel-proxy/pkg/metrics"
	"github.com/klamas1/redis-sentinel-proxy/pkg/pool"
	"github.com/klamas1/redis-sentinel-proxy/pkg/proxy"
	"github.com/klamas1/redis-sentinel-proxy/pkg/resolver"
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
		debug                = false
		logLevel             = "info"
		metricsAddr          = ":9090"

		// Timeout config
		connectTimeout = 1 * time.Second
		readTimeout    = 30 * time.Second
		writeTimeout   = 30 * time.Second

		// Circuit Breaker config
		cbFailureThreshold = 5
		cbTimeout          = 30 * time.Second

		// Retry config
		retryMax          = 3
		retryInitialDelay = 100 * time.Millisecond

		// Health check config
		healthCheckEnabled  = true
		healthCheckInterval = 5 * time.Second
		healthCheckTimeout  = 1 * time.Second

		// Connection pool config
		poolMaxIdle     = 10
		poolMaxActive   = 100
		poolIdleTimeout = 30 * time.Second
		poolMaxLifetime = 5 * time.Minute
	)

	flag.StringVar(&localAddr, "listen", localAddr, "local address for master proxy")
	flag.StringVar(&replicaAddr, "replica-listen", replicaAddr, "local address for replica proxy")
	flag.StringVar(&sentinelAddr, "sentinel", sentinelAddr, "remote address")
	flag.StringVar(&masterName, "master", masterName, "name of the master redis node")
	flag.StringVar(&password, "password", password, "redis password")
	flag.IntVar(&masterResolveRetries, "resolve-retries", masterResolveRetries, "number of consecutive retries of the redis master node resolve")
	flag.StringVar(&balancingType, "balancing", balancingType, "balancing type for replicas: round-robin or leastconn")
	flag.BoolVar(&debug, "debug", debug, "enable debug logging")
	flag.StringVar(&logLevel, "log-level", logLevel, "log level: debug/info/warn/error")
	flag.StringVar(&metricsAddr, "metrics-addr", metricsAddr, "address for metrics export")

	// Timeout flags
	flag.DurationVar(&connectTimeout, "connect-timeout", connectTimeout, "connection timeout")
	flag.DurationVar(&readTimeout, "read-timeout", readTimeout, "read timeout")
	flag.DurationVar(&writeTimeout, "write-timeout", writeTimeout, "write timeout")

	// Circuit Breaker flags
	flag.IntVar(&cbFailureThreshold, "cb-failure-threshold", cbFailureThreshold, "circuit breaker failure threshold")
	flag.DurationVar(&cbTimeout, "cb-timeout", cbTimeout, "circuit breaker timeout before half-open")

	// Retry flags
	flag.IntVar(&retryMax, "retry-max", retryMax, "maximum number of retry attempts")
	flag.DurationVar(&retryInitialDelay, "retry-initial-delay", retryInitialDelay, "initial delay for retry backoff")

	// Health check flags
	flag.BoolVar(&healthCheckEnabled, "health-check-enabled", healthCheckEnabled, "enable health checking for replicas")
	flag.DurationVar(&healthCheckInterval, "health-check-interval", healthCheckInterval, "health check interval")
	flag.DurationVar(&healthCheckTimeout, "health-check-timeout", healthCheckTimeout, "health check timeout")

	// Connection pool flags
	flag.IntVar(&poolMaxIdle, "pool-max-idle", poolMaxIdle, "max idle connections in pool")
	flag.IntVar(&poolMaxActive, "pool-max-active", poolMaxActive, "max active connections in pool")
	flag.DurationVar(&poolIdleTimeout, "pool-idle-timeout", poolIdleTimeout, "idle connection timeout")
	flag.DurationVar(&poolMaxLifetime, "pool-max-lifetime", poolMaxLifetime, "max connection lifetime")

	flag.Parse()

	if envPassword := os.Getenv("SENTINEL_PASSWORD"); envPassword != "" {
		password = envPassword
	}

	// Parse log level
	level, err := logger.ParseLogLevel(logLevel)
	if err != nil {
		level = logger.InfoLevel
	}

	// Create logger
	var logOutput interface{}
	if debug {
		level = logger.DebugLevel
	}
	logOutput = os.Stdout
	if level == logger.ErrorLevel {
		logOutput = os.Stderr
	}
	appLogger := logger.NewLogger(level, logOutput.(interface{ Write([]byte) (int, error) }))

	// Create metrics
	appMetrics := metrics.NewMetrics()

	bt := parseBalancingType(balancingType)

	// Create pool config
	poolConfig := pool.PoolConfig{
		MaxIdle:         poolMaxIdle,
		MaxActive:       poolMaxActive,
		IdleTimeout:     poolIdleTimeout,
		MaxConnLifetime: poolMaxLifetime,
	}

	// Create proxy config
	proxyConfig := proxy.ProxyConfig{
		CircuitBreakerConfig: proxy.Config{
			FailureThreshold:    cbFailureThreshold,
			SuccessThreshold:    2,
			Timeout:             cbTimeout,
			HalfOpenMaxRequests: 3,
		},
		RetryConfig: proxy.RetryConfig{
			MaxRetries:   retryMax,
			InitialDelay: retryInitialDelay,
			MaxDelay:     2 * time.Second,
			Multiplier:   2.0,
			JitterFactor: 0.1,
		},
		PoolConfig:     poolConfig,
		ConnectTimeout: connectTimeout,
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
	}

	// Create health check config if enabled
	var healthConfig *resolver.HealthConfig
	if healthCheckEnabled {
		hc := resolver.HealthConfig{
			Interval:           healthCheckInterval,
			Timeout:            healthCheckTimeout,
			UnhealthyThreshold: 3,
			HealthyThreshold:   2,
		}
		healthConfig = &hc
	}

	if err := runProxying(localAddr, replicaAddr, sentinelAddr, password, masterName, masterResolveRetries, bt, metricsAddr, appLogger, appMetrics, proxyConfig, healthConfig); err != nil {
		appLogger.Error("Fatal: %s", err)
		os.Exit(1)
	}
	appLogger.Info("Exiting...")
}

func parseBalancingType(s string) resolver.BalancingType {
	switch s {
	case "leastconn":
		return resolver.LeastConn
	default:
		return resolver.RoundRobin
	}
}

func runProxying(localAddr, replicaAddr, sentinelAddr, password string, masterName string, masterResolveRetries int, bt resolver.BalancingType, metricsAddr string, appLogger logger.Logger, appMetrics *metrics.Metrics, proxyConfig proxy.ProxyConfig, healthConfig *resolver.HealthConfig) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	laddr := resolveTCPAddr(localAddr, appLogger)
	raddr := resolveTCPAddr(replicaAddr, appLogger)
	saddr := resolveTCPAddr(sentinelAddr, appLogger)

	sentinelResolver := resolver.NewRedisSentinelResolver(masterName, saddr, password, masterResolveRetries, bt, appLogger, healthConfig, appMetrics)

	masterProxy := proxy.NewRedisSentinelProxy(laddr, sentinelResolver, "master", appLogger, proxyConfig, appMetrics)
	replicaProxy := proxy.NewRedisSentinelProxy(raddr, sentinelResolver, "replica", appLogger, proxyConfig, appMetrics)

	// Start metrics server
	metricsServer := &http.Server{
		Addr:         metricsAddr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Create metrics handler
	http.Handle("/metrics", metrics.NewMetricsHandler(appMetrics))

	eg, ctx := errgroup.WithContext(ctx)

	// Start metrics server
	eg.Go(func() error {
		appLogger.Info("Starting metrics server on %s", metricsAddr)
		err := metricsServer.ListenAndServe()
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	eg.Go(func() error { return sentinelResolver.UpdateLoop(ctx) })
	eg.Go(func() error { return masterProxy.Run(ctx) })
	eg.Go(func() error { return replicaProxy.Run(ctx) })

	err := eg.Wait()

	// Graceful shutdown of metrics server
	if errShutdown := metricsServer.Shutdown(ctx); errShutdown != nil {
		appLogger.Error("Failed to shutdown metrics server: %s", errShutdown)
	}

	return err
}

func resolveTCPAddr(addr string, appLogger logger.Logger) *net.TCPAddr {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		appLogger.Error("Fatal - Failed resolving tcp address: %s", err)
		os.Exit(1)
	}
	return tcpAddr
}
