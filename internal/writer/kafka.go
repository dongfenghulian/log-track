package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
	"github.com/segmentio/kafka-go"
)

// KafkaWriter wraps a kafka-go Writer per topic, lazily created.
//
// We use one Writer per topic so that batching is per-topic — segmentio/kafka-go batches
// across the same Writer instance, and mixing topics on one writer would force per-message round trips.
type KafkaWriter struct {
	brokers      []string
	batchSize    int
	batchTimeout time.Duration
	writeTimeout time.Duration

	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func NewKafkaWriter(brokers []string, batchSize int, batchTimeout, writeTimeout time.Duration) *KafkaWriter {
	if writeTimeout <= 0 {
		writeTimeout = 2 * time.Second
	}
	return &KafkaWriter{
		brokers:      brokers,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		writeTimeout: writeTimeout,
		writers:      map[string]*kafka.Writer{},
	}
}

func (k *KafkaWriter) writerFor(topic string) *kafka.Writer {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.writers[topic]; ok {
		return w
	}
	// RequireOne: leader-only ack. Throughput-optimized.
	// LogTrack is an observability system; the fallback file path covers durability.
	// If you need at-least-once on the broker side, switch to RequireAll.
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    k.batchSize,
		BatchTimeout: k.batchTimeout,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		WriteTimeout: k.writeTimeout,
	}
	k.writers[topic] = w
	return w
}

// Write serializes the envelope and writes it to its destination topic.
//
// Per spec: built-in handlers may transform data before writing, but at the manager layer we always
// serialize the full envelope. Handlers wrap us so they own that decision.
func (k *KafkaWriter) Write(env *envelope.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	w := k.writerFor(env.Topic)
	ctx, cancel := context.WithTimeout(context.Background(), k.writeTimeout)
	defer cancel()
	msg := kafka.Message{Value: body}
	if env.TraceID != "" {
		msg.Key = []byte(env.TraceID)
	}
	start := time.Now()
	err = w.WriteMessages(ctx, msg)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		metrics.KafkaWrite(env.Topic, "error", elapsed)
		return err
	}
	metrics.KafkaWrite(env.Topic, "success", elapsed)
	return nil
}

// Flush waits for in-flight batches to land. kafka-go's WriteMessages already blocks until ack
// when Async=false, so this is a courtesy method that returns once ctx fires; in practice we just
// honor the deadline and return.
func (k *KafkaWriter) Flush(ctx context.Context) error {
	// kafka-go has no explicit Flush API for sync Writers; relying on WriteMessages' synchronous
	// semantics. We block briefly to let any reordered packets settle.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

// Close closes all per-topic writers.
func (k *KafkaWriter) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	var firstErr error
	for _, w := range k.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	k.writers = nil
	return firstErr
}

// Probe verifies that at least one broker is reachable by establishing a TCP control connection
// and reading the broker list. It writes nothing — meant for health-recovery checks where we
// don't want to inject probe messages into business topics.
func (k *KafkaWriter) Probe(ctx context.Context) error {
	if len(k.brokers) == 0 {
		return errors.New("no brokers configured")
	}
	d := &kafka.Dialer{Timeout: 3 * time.Second}
	var lastErr error
	for _, addr := range k.brokers {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		// Reading broker metadata exercises the Kafka protocol, not just the TCP socket;
		// distinguishes "TCP open but service down" from "fully healthy".
		_, err = conn.Brokers()
		_ = conn.Close()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("kafka probe failed: %w", lastErr)
}
