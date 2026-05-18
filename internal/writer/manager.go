// Package writer persists envelopes to Kafka, falling back to local files when Kafka is unhealthy.
package writer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// Manager routes writes to Kafka or fallback file based on Kafka health.
//
// Health flips off on the first Kafka write error and stays off until the recovery probe succeeds.
// Recovery probe runs every recoveryInterval; it tries to write a single fallback record back to Kafka,
// flips health on if that succeeds, and continues draining the fallback queue.
type Manager struct {
	kafka      *KafkaWriter
	fallback   *FallbackWriter
	healthy    atomic.Bool
	logger     *slog.Logger
	stopOnce   sync.Once
	stopCh     chan struct{}
	recoveryWG sync.WaitGroup

	flushTimeout time.Duration
}

const recoveryInterval = 10 * time.Second

// NewManager wires the two writers and starts the background recovery goroutine.
func NewManager(kafka *KafkaWriter, fallback *FallbackWriter, flushTimeout time.Duration, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		kafka:        kafka,
		fallback:     fallback,
		logger:       logger,
		stopCh:       make(chan struct{}),
		flushTimeout: flushTimeout,
	}
	m.healthy.Store(true)
	m.recoveryWG.Add(1)
	go m.recoveryLoop()
	return m
}

// Write persists one envelope. Never returns error to callers; failures are logged and routed to fallback.
func (m *Manager) Write(env *envelope.Envelope) {
	if m.healthy.Load() {
		if err := m.kafka.Write(env); err != nil {
			m.logger.Warn("kafka write failed; switching to fallback",
				"topic", env.Topic, "err", err)
			m.healthy.Store(false)
			metrics.KafkaHealthy(false)
			m.writeFallback(env)
			return
		}
		return
	}
	m.writeFallback(env)
}

func (m *Manager) writeFallback(env *envelope.Envelope) {
	if err := m.fallback.Write(env); err != nil {
		m.logger.Error("fallback write failed; message lost",
			"topic", env.Topic, "err", err)
	}
}

func (m *Manager) recoveryLoop() {
	defer m.recoveryWG.Done()
	ticker := time.NewTicker(recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if m.healthy.Load() {
				continue
			}
			m.tryRecover()
		}
	}
}

// tryRecover replays one fallback record. If Kafka accepts it, mark healthy and drain the rest.
// Drain is bounded by recoveryInterval so we don't hold the loop hostage.
func (m *Manager) tryRecover() {
	probe, ok := m.fallback.Peek()
	if !ok {
		// Nothing to replay; just probe Kafka with a no-op (we have no obvious probe message).
		// Mark healthy optimistically; next real write will validate.
		m.healthy.Store(true)
		metrics.KafkaHealthy(true)
		m.logger.Info("kafka health flipped to true (no fallback to replay)")
		return
	}
	if err := m.kafka.Write(probe.Env); err != nil {
		metrics.FallbackReplay("error")
		m.logger.Warn("kafka still unhealthy", "err", err)
		return
	}
	m.fallback.Ack(probe)
	metrics.FallbackReplay("success")
	m.healthy.Store(true)
	metrics.KafkaHealthy(true)
	m.logger.Info("kafka recovered; draining fallback")

	// Drain the rest with a deadline so we don't starve the loop.
	deadline := time.Now().Add(recoveryInterval - 2*time.Second)
	for time.Now().Before(deadline) {
		rec, ok := m.fallback.Peek()
		if !ok {
			return
		}
		if err := m.kafka.Write(rec.Env); err != nil {
			metrics.FallbackReplay("error")
			m.logger.Warn("kafka write failed during drain; staying unhealthy", "err", err)
			m.healthy.Store(false)
			metrics.KafkaHealthy(false)
			return
		}
		m.fallback.Ack(rec)
		metrics.FallbackReplay("success")
	}
}

// Shutdown drains the queue, flushes Kafka, then closes the fallback writer.
// Order matters: flush Kafka first so anything still in producer batches goes through;
// then any unsent envelopes (held by callers, not queued here) would already have hit fallback.
func (m *Manager) Shutdown(ctx context.Context) {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.recoveryWG.Wait()

	flushCtx, cancel := context.WithTimeout(ctx, m.flushTimeout)
	defer cancel()
	if err := m.kafka.Flush(flushCtx); err != nil {
		m.logger.Warn("kafka flush during shutdown returned error", "err", err)
	}
	if err := m.kafka.Close(); err != nil {
		m.logger.Warn("kafka close error", "err", err)
	}
	if err := m.fallback.Close(); err != nil {
		m.logger.Warn("fallback close error", "err", err)
	}
}

// SerializeEnvelope is exposed for handlers that need to write the envelope back to Kafka as-is (passthrough).
func SerializeEnvelope(env *envelope.Envelope) ([]byte, error) {
	return json.Marshal(env)
}
