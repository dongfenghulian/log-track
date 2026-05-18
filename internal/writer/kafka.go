package writer

import (
	"context"
	"encoding/json"
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

	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func NewKafkaWriter(brokers []string, batchSize int, batchTimeout time.Duration) *KafkaWriter {
	return &KafkaWriter{
		brokers:      brokers,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		writers:      map[string]*kafka.Writer{},
	}
}

func (k *KafkaWriter) writerFor(topic string) *kafka.Writer {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    k.batchSize,
		BatchTimeout: k.batchTimeout,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
