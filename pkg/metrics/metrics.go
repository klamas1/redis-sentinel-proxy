package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics структура для управления метриками
type Metrics struct {
	// Active connections - Gauge
	activeConnections *prometheus.GaugeVec

	// Total connections - Counter
	totalConnections *prometheus.CounterVec

	// Failed connections - Counter
	failedConnections *prometheus.CounterVec

	// Bytes in - Counter
	bytesIn *prometheus.CounterVec

	// Bytes out - Counter
	bytesOut *prometheus.CounterVec

	// Timeout errors - Counter
	timeoutErrors *prometheus.CounterVec

	// Connection errors - Counter
	connectionErrors *prometheus.CounterVec

	// Replica health - Gauge
	replicaHealth *prometheus.GaugeVec

	// Circuit breaker state - Gauge
	circuitBreakerState *prometheus.GaugeVec

	// Request duration - Histogram
	requestDuration *prometheus.HistogramVec

	// Pool stats - Gauge
	poolStats *prometheus.GaugeVec

	mu sync.RWMutex
}

// NewMetrics создает новую структуру метрик
func NewMetrics() *Metrics {
	return &Metrics{
		activeConnections: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_proxy_active_connections",
			Help: "Number of active connections",
		}, []string{"type"}),

		totalConnections: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_total_connections",
			Help: "Total number of connections",
		}, []string{"type"}),

		failedConnections: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_failed_connections",
			Help: "Number of failed connections",
		}, []string{"type"}),

		bytesIn: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_bytes_in",
			Help: "Total bytes received",
		}, []string{"type"}),

		bytesOut: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_bytes_out",
			Help: "Total bytes sent",
		}, []string{"type"}),

		timeoutErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_timeout_errors",
			Help: "Number of timeout errors",
		}, []string{"type"}),

		connectionErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_proxy_connection_errors",
			Help: "Number of connection errors",
		}, []string{"type", "error_type"}),

		replicaHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_proxy_replica_health",
			Help: "Health status of replicas (1=healthy, 0=unhealthy)",
		}, []string{"replica"}),

		circuitBreakerState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_proxy_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		}, []string{"type"}),

		requestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "redis_proxy_request_duration_seconds",
			Help: "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),

		poolStats: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_proxy_pool_stats",
			Help: "Connection pool statistics",
		}, []string{"type", "stat"}),
	}
}

// RegisterPrometheus регистрирует метрики в указанном регистраторе
func RegisterPrometheus(registry prometheus.Registerer) {
	// Метрики уже зарегистрированы через promauto
	// Эта функция оставлена для совместимости и возможного расширения
	_ = registry
}

// IncActiveConnections увеличивает счетчик активных соединений
func (m *Metrics) IncActiveConnections(t string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.activeConnections.WithLabelValues(t).Inc()
}

// DecActiveConnections уменьшает счетчик активных соединений
func (m *Metrics) DecActiveConnections(t string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.activeConnections.WithLabelValues(t).Dec()
}

// IncTotalConnections увеличивает счетчик общих соединений
func (m *Metrics) IncTotalConnections(t string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.totalConnections.WithLabelValues(t).Inc()
}

// IncFailedConnections увеличивает счетчик неудачных соединений
func (m *Metrics) IncFailedConnections(t string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.failedConnections.WithLabelValues(t).Inc()
}

// AddBytesIn добавляет количество полученных байт
func (m *Metrics) AddBytesIn(t string, n float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.bytesIn.WithLabelValues(t).Add(n)
}

// AddBytesOut добавляет количество отправленных байт
func (m *Metrics) AddBytesOut(t string, n float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.bytesOut.WithLabelValues(t).Add(n)
}

// IncTimeoutErrors увеличивает счетчик таймаутов
func (m *Metrics) IncTimeoutErrors(t string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.timeoutErrors.WithLabelValues(t).Inc()
}

// IncConnectionErrors увеличивает счетчик ошибок соединений
func (m *Metrics) IncConnectionErrors(t string, errorType string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.connectionErrors.WithLabelValues(t, errorType).Inc()
}

// SetReplicaHealth устанавливает статус здоровья реплики
func (m *Metrics) SetReplicaHealth(replica string, healthy bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value := 0.0
	if healthy {
		value = 1.0
	}
	m.replicaHealth.WithLabelValues(replica).Set(value)
}

// SetCircuitBreakerState устанавливает состояние circuit breaker
func (m *Metrics) SetCircuitBreakerState(t string, state int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.circuitBreakerState.WithLabelValues(t).Set(float64(state))
}

// ObserveRequestDuration наблюдает длительность запроса
func (m *Metrics) ObserveRequestDuration(t string, duration time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.requestDuration.WithLabelValues(t).Observe(duration.Seconds())
}

// SetPoolStats устанавливает статистику пула соединений
func (m *Metrics) SetPoolStats(t string, stat string, value float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.poolStats.WithLabelValues(t, stat).Set(value)
}

// NewMetricsHandler создает HTTP handler для метрик Prometheus
func NewMetricsHandler(m *Metrics) http.Handler {
	// Используем стандартный handler от prometheus
	// Все метрики уже зарегистрированы через promauto
	return promhttp.Handler()
}
