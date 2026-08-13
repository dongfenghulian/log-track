// Package metrics defines Prometheus metrics for the gateway and exposes the /metrics handler.
//
// All metrics are registered to the default Prometheus registry. Modules call the package-level
// helpers (ConnInc, MessageObserved, etc.); they do not depend on prometheus types directly.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	connections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logtrack_gateway_connections",
		Help: "Number of active TCP connections to the gateway.",
	})

	messagesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logtrack_gateway_messages_total",
		Help: "Number of messages routed, by topic and outcome.",
	}, []string{"topic", "outcome"})

	queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logtrack_gateway_queue_depth",
		Help: "Current depth of the in-memory message queue.",
	})

	queueDropsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logtrack_gateway_queue_drops_total",
		Help: "Number of messages dropped due to back pressure (currently always 0; queue blocks).",
	})

	kafkaWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logtrack_kafka_writes_total",
		Help: "Number of Kafka write attempts, by topic and outcome.",
	}, []string{"topic", "outcome"})

	kafkaWriteLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logtrack_kafka_write_latency_seconds",
		Help:    "Latency of Kafka write attempts, by topic.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms .. ~4s
	}, []string{"topic"})

	kafkaHealthy = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logtrack_kafka_healthy",
		Help: "1 if the writer manager considers Kafka healthy, 0 otherwise.",
	})

	fallbackFiles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "logtrack_fallback_files",
		Help: "Number of rolled fallback files (.log.done) waiting to replay.",
	})

	fallbackWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logtrack_fallback_writes_total",
		Help: "Number of envelopes written to the fallback writer.",
	})

	fallbackReplaysTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logtrack_fallback_replays_total",
		Help: "Number of fallback envelopes replayed to Kafka, by outcome.",
	}, []string{"outcome"},
	)
)

func init() {
	prometheus.MustRegister(
		connections,
		messagesTotal,
		queueDepth,
		queueDropsTotal,
		kafkaWrites,
		kafkaWriteLatency,
		kafkaHealthy,
		fallbackFiles,
		fallbackWritesTotal,
		fallbackReplaysTotal,
	)
	kafkaHealthy.Set(1)
}

// Handler returns the HTTP handler for /metrics.
func Handler() http.Handler { return promhttp.Handler() }

// ConnInc / ConnDec track active TCP connections.
func ConnInc()      { connections.Inc() }
func ConnDec()      { connections.Dec() }

// MessageObserved records the per-topic processing outcome.
//
// outcome ∈ {"handled", "passthrough", "invalid", "version_dropped"}
func MessageObserved(topic, outcome string) {
	messagesTotal.WithLabelValues(topic, outcome).Inc()
}

// QueueDepthSet should be called periodically (or on every enqueue, cheap enough).
func QueueDepthSet(n int) { queueDepth.Set(float64(n)) }

// QueueDropInc — kept for future back-pressure mode.
func QueueDropInc() { queueDropsTotal.Inc() }

// KafkaWrite reports one write attempt.
//
// outcome ∈ {"success", "error"}
func KafkaWrite(topic, outcome string, seconds float64) {
	kafkaWrites.WithLabelValues(topic, outcome).Inc()
	kafkaWriteLatency.WithLabelValues(topic).Observe(seconds)
}

// KafkaHealthy flips the health gauge.
func KafkaHealthy(ok bool) {
	if ok {
		kafkaHealthy.Set(1)
	} else {
		kafkaHealthy.Set(0)
	}
}

// FallbackFilesSet should be called by the fallback writer after rotation.
func FallbackFilesSet(n int) { fallbackFiles.Set(float64(n)) }

// FallbackFilesGauge returns the underlying gauge for use in tests.
func FallbackFilesGauge() prometheus.Gauge { return fallbackFiles }

// FallbackWriteInc on every fallback write.
func FallbackWriteInc() { fallbackWritesTotal.Inc() }

// FallbackReplay records a replay attempt outcome.
//
// outcome ∈ {"success", "error"}
func FallbackReplay(outcome string) {
	fallbackReplaysTotal.WithLabelValues(outcome).Inc()
}
