// Package logtrack is the LogTrack client SDK.
//
// Usage:
//
//	logtrack.Init(&logtrack.Config{
//	    GatewayAddr: "logtrack-gateway:8080",
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
	defaultMaxConns       = 4
	defaultConnectTimeout = 3 * time.Second
	defaultWriteTimeout   = 1 * time.Second
)

// Config is supplied at Init time. Only GatewayAddr and ServiceName are required.
type Config struct {
	GatewayAddr    string
	ServiceName    string
	MaxConns       int           // 1..4, defaults to 4
	ConnectTimeout time.Duration // defaults to 3s
	WriteTimeout   time.Duration // defaults to 1s
	Logger         *slog.Logger  // defaults to slog.Default()
}

// Client is a long-lived sender. Most apps use the package-level singleton via Init.
type Client struct {
	addr           string
	service        string
	host           string
	connectTimeout time.Duration
	writeTimeout   time.Duration
	logger         *slog.Logger
	shards         []*shardConn
}

type shardConn struct {
	mu   sync.Mutex
	conn net.Conn
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
	defaultClient = c
	defaultMu.Unlock()
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
	if cfg.GatewayAddr == "" {
		return nil, errors.New("logtrack: GatewayAddr is required")
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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	host, _ := os.Hostname()

	shards := make([]*shardConn, maxConns)
	for i := range shards {
		shards[i] = &shardConn{}
	}
	return &Client{
		addr:           cfg.GatewayAddr,
		service:        cfg.ServiceName,
		host:           host,
		connectTimeout: connectTO,
		writeTimeout:   writeTO,
		logger:         logger,
		shards:         shards,
	}, nil
}

// Close releases all shard connections. Safe to call multiple times.
func (c *Client) Close() {
	for _, s := range c.shards {
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.mu.Unlock()
	}
}

func (c *Client) shardIndex(traceID string) int {
	if traceID == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(traceID))
	return int(h.Sum32() % uint32(len(c.shards)))
}

// send is the single entry point all helpers funnel into.
func (c *Client) send(topic string, data any, traceID string) {
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
	env := envelope.Envelope{
		Version:   envelope.Version,
		Topic:     topic,
		Service:   c.service,
		Host:      c.host,
		Timestamp: time.Now().UnixMilli(),
		TraceID:   traceID,
		Data:      payload,
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

	idx := c.shardIndex(traceID)
	s := c.shards[idx]
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
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
			return
		}
		s.conn = conn
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
