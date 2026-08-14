// Package logtrack is the LogTrack client SDK.
//
// Usage:
//
//	logtrack.Init(&logtrack.Config{
//	    GatewayAddr: "log-track:9583", // optional, this is the default
//	    ServiceName: "loan-backend",
//	})
//	defer logtrack.Close()
//
//	logtrack.InboundHTTP(&logtrack.InboundHTTPLog{...})
package logtrack

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

const (
	defaultGatewayAddr    = "log-track:9583"
	defaultMaxConns       = 4
	defaultConnectTimeout = 3 * time.Second
	defaultWriteTimeout   = 1 * time.Second
	defaultFailureBackoff = 5 * time.Second
)

// Config is supplied at Init time. Only GatewayAddr and ServiceName are required.
type Config struct {
	GatewayAddr    string
	ServiceName    string
	MaxConns       int           // per-pool conn count: this many event-tracks conns + this many normal conns; 1..16, defaults to 4
	ConnectTimeout time.Duration // defaults to 3s
	WriteTimeout   time.Duration // defaults to 1s
	FailureBackoff time.Duration // defaults to 5s; <=0 uses default
	Logger         *slog.Logger  // defaults to slog.Default()
}

// Client is a long-lived sender. Most apps use the package-level singleton via Init.
type Client struct {
	addr           string
	service        string
	host           string
	connectTimeout time.Duration
	writeTimeout   time.Duration
	failureBackoff time.Duration
	logger         *slog.Logger
	eventShards    []*shardConn
	normalShards   []*shardConn
	closed         bool
	closeMu        sync.RWMutex
}

type shardConn struct {
	mu          sync.Mutex
	conn        net.Conn
	nextAttempt time.Time
}

var (
	defaultClient *Client
	defaultMu     sync.RWMutex
)

// Init sets up the package-level singleton. Calling Init twice replaces the previous client.
func Init(cfg *Config) error {
	c, err := New(cfg)
	if err != nil {
		return err
	}
	defaultMu.Lock()
	old := defaultClient
	defaultClient = c
	defaultMu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

// Close closes the package-level singleton's connections.
func Close() {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultClient != nil {
		defaultClient.Close()
		defaultClient = nil
	}
}

func client() *Client {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return defaultClient
}

// New constructs a Client without touching package state. Useful in tests.
func New(cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("logtrack: nil config")
	}
	addr := cfg.GatewayAddr
	if addr == "" {
		addr = defaultGatewayAddr
	}
	if cfg.ServiceName == "" {
		return nil, errors.New("logtrack: ServiceName is required")
	}
	maxConns := cfg.MaxConns
	if maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	if maxConns > 16 {
		maxConns = 16
	}
	connectTO := cfg.ConnectTimeout
	if connectTO <= 0 {
		connectTO = defaultConnectTimeout
	}
	writeTO := cfg.WriteTimeout
	if writeTO <= 0 {
		writeTO = defaultWriteTimeout
	}
	failureBackoff := cfg.FailureBackoff
	if failureBackoff <= 0 {
		failureBackoff = defaultFailureBackoff
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	host, _ := os.Hostname()

	eventShards := make([]*shardConn, maxConns)
	normalShards := make([]*shardConn, maxConns)
	for i := range eventShards {
		eventShards[i] = &shardConn{}
		normalShards[i] = &shardConn{}
	}
	return &Client{
		addr:           addr,
		service:        cfg.ServiceName,
		host:           host,
		connectTimeout: connectTO,
		writeTimeout:   writeTO,
		failureBackoff: failureBackoff,
		logger:         logger,
		eventShards:    eventShards,
		normalShards:   normalShards,
	}, nil
}

// Close releases all shard connections. Safe to call multiple times.
func (c *Client) Close() {
	c.closeMu.Lock()
	c.closed = true
	c.closeMu.Unlock()

	for _, s := range c.eventShards {
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.mu.Unlock()
	}
	for _, s := range c.normalShards {
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.mu.Unlock()
	}
}

func (c *Client) shardIndex(traceID string, size int) int {
	if traceID == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(traceID))
	return int(h.Sum32() % uint32(size))
}

// send is the single entry point all helpers funnel into.
func (c *Client) send(topic string, data any, traceID, partitionKey string) {
	c.closeMu.RLock()
	if c.closed {
		c.closeMu.RUnlock()
		return
	}
	c.closeMu.RUnlock()

	var shards []*shardConn
	if topic == envelope.TopicEventTracks {
		shards = c.eventShards
	} else {
		shards = c.normalShards
	}
	idx := c.shardIndex(traceID, len(shards))
	s := shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	backoffEnabled := topic != envelope.TopicEventTracks
	if backoffEnabled && now.Before(s.nextAttempt) {
		c.logger.Debug("logtrack: send skipped during failure backoff",
			"stage", "backoff",
			"topic", topic,
			"service", c.service,
			"trace_id", traceID,
			"shard", idx,
			"retry_at", s.nextAttempt.Format(time.RFC3339Nano))
		return
	}

	payload, err := json.Marshal(data)
	if err != nil {
		c.logger.Error("logtrack: serialize failed",
			"stage", "serialize",
			"topic", topic,
			"service", c.service,
			"trace_id", traceID,
			"err", err)
		return
	}
	now = time.Now()
	env := envelope.Envelope{
		Version:      envelope.Version,
		Topic:        topic,
		Service:      c.service,
		Host:         c.host,
		Timestamp:    now.UnixMilli(),
		TimestampAt:  envelope.TimestampAtFromTime(now),
		TraceID:      traceID,
		PartitionKey: partitionKey,
		Data:         payload,
	}
	frame, err := encodeFrame(&env)
	if err != nil {
		c.logger.Error("logtrack: serialize failed",
			"stage", "serialize",
			"topic", topic,
			"service", c.service,
			"trace_id", traceID,
			"err", err)
		return
	}

	if s.conn == nil {
		c.closeMu.RLock()
		closed := c.closed
		c.closeMu.RUnlock()
		if closed {
			return
		}
		conn, err := net.DialTimeout("tcp", c.addr, c.connectTimeout)
		if err != nil {
			c.logger.Warn("logtrack: dial failed",
				"stage", "dial",
				"topic", topic,
				"service", c.service,
				"trace_id", traceID,
				"shard", idx,
				"size", len(frame),
				"err", err)
			if backoffEnabled {
				s.nextAttempt = time.Now().Add(c.failureBackoff)
			}
			return
		}
		s.conn = conn
		s.nextAttempt = time.Time{}
	}

	_ = s.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	if _, err := s.conn.Write(frame); err != nil {
		c.logger.Warn("logtrack: write failed",
			"stage", "write",
			"topic", topic,
			"service", c.service,
			"trace_id", traceID,
			"shard", idx,
			"size", len(frame),
			"err", err)
		_ = s.conn.Close()
		s.conn = nil
		if backoffEnabled {
			s.nextAttempt = time.Now().Add(c.failureBackoff)
		}
	}
}

// encodeFrame serializes the envelope and prepends a 4-byte big-endian length prefix.
func encodeFrame(env *envelope.Envelope) ([]byte, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	return frame, nil
}
