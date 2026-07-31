// Package server hosts the TCP listener and per-connection read loop.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/internal/queue"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type Config struct {
	Address         string
	MaxConnections  int
	MaxMessageSize  int
	ConnReadTimeout time.Duration // applied during shutdown to bound how long we read from existing conns
}

type Server struct {
	cfg    Config
	queue  *queue.Queue
	logger *slog.Logger

	listener  net.Listener
	connsMu   sync.Mutex
	conns     map[net.Conn]struct{}
	connCount atomic.Int64

	shutdownMode atomic.Bool // when true, conn read loops use ConnReadTimeout
}

func New(cfg Config, q *queue.Queue, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:    cfg,
		queue:  q,
		logger: logger,
		conns:  make(map[net.Conn]struct{}),
	}
}

// Start begins accepting connections. Returns when the listener errors or is closed.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return err
	}
	s.listener = ln
	s.logger.Info("server listening", "addr", s.cfg.Address)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.logger.Warn("accept error", "err", err)
			continue
		}
		if int(s.connCount.Add(1)) > s.cfg.MaxConnections {
			s.connCount.Add(-1)
			_ = conn.Close()
			s.logger.Warn("connection limit reached, dropping", "addr", conn.RemoteAddr())
			continue
		}
		s.connsMu.Lock()
		s.conns[conn] = struct{}{}
		s.connsMu.Unlock()
		metrics.ConnInc()
		go s.serve(conn)
	}
}

func (s *Server) serve(conn net.Conn) {
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
		s.connCount.Add(-1)
		metrics.ConnDec()
		_ = conn.Close()
	}()

	for {
		if s.shutdownMode.Load() {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ConnReadTimeout))
		}
		env, err := readFrame(conn, s.cfg.MaxMessageSize)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.logger.Debug("conn read ended", "err", err, "remote", conn.RemoteAddr())
			}
			return
		}
		if env.Version != envelope.Version {
			s.logger.Warn("dropping message with unknown version",
				"version", env.Version, "topic", env.Topic, "remote", conn.RemoteAddr())
			metrics.MessageObserved(env.Topic, "version_dropped")
			continue
		}
		s.queue.Enqueue(env)
	}
}

// Shutdown stops accepting new connections, then waits for existing ones to finish (bounded by ConnReadTimeout).
func (s *Server) Shutdown(ctx context.Context) {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.shutdownMode.Store(true)

	// Wait for conns to drain. Loop until conn count hits zero or ctx fires.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if s.connCount.Load() == 0 {
			return
		}
		select {
		case <-ctx.Done():
			s.forceCloseAll()
			return
		case <-tick.C:
		}
	}
}

func (s *Server) forceCloseAll() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
}
