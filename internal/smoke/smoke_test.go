// Smoke-tests the gateway end-to-end without Kafka.
//
// Strategy: point Kafka brokers to an unreachable address so the manager flips to fallback
// on the very first write. Then send envelopes via the SDK and assert they land in the fallback dir.
//
//go:build smoke

package smoke

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dongfenghulian/log-track/internal/config"
	"github.com/dongfenghulian/log-track/internal/handler/passthrough"
	"github.com/dongfenghulian/log-track/internal/queue"
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/server"
	"github.com/dongfenghulian/log-track/internal/writer"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"

	_ "github.com/dongfenghulian/log-track/internal/handler"
)

func TestSmokeFallbackPath(t *testing.T) {
	tmp := t.TempDir()

	t.Setenv("LOG_TRACK_KAFKA_BROKERS", "127.0.0.1:1") // unreachable → forces fallback
	t.Setenv("LOG_TRACK_FALLBACK_DATA_DIR", tmp)
	t.Setenv("LOG_TRACK_FALLBACK_MAX_FILE_SIZE", "1024") // small → exercises rotation
	t.Setenv("LOG_TRACK_FALLBACK_MAX_FILES", "100")

	cfg := config.Load()

	// Pick a free port for the test (overrides LOG_TRACK_SERVER_ADDRESS default).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	cfg.Server.Address = addr

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	kw := writer.NewKafkaWriter(cfg.Kafka.Brokers, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout)
	fw, err := writer.NewFallbackWriter(cfg.Fallback.DataDir, cfg.Fallback.MaxFileSize, cfg.Fallback.MaxFiles)
	if err != nil {
		t.Fatalf("fallback init: %v", err)
	}
	mgr := writer.NewManager(kw, fw, cfg.Shutdown.KafkaFlushTimeout, logger)
	writeradapter.Set(mgr.Write)

	pass := &passthrough.Handler{}
	q := queue.New(cfg.Server.QueueSize)
	q.Start(4, func(env *envelope.Envelope) {
		h, ok := router.Lookup(env.Topic)
		if !ok {
			_ = pass.Handle(env)
			return
		}
		_ = h.Handle(env)
	})

	srv := server.New(server.Config{
		Address:         cfg.Server.Address,
		MaxConnections:  cfg.Server.MaxConnections,
		MaxMessageSize:  cfg.Server.MaxMessageSize,
		ConnReadTimeout: cfg.Shutdown.ConnReadTimeout,
	}, q, logger)
	go func() { _ = srv.Start() }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		q.Close()
	})

	waitForListen(t, addr)

	if err := logtrack.Init(&logtrack.Config{
		GatewayAddr: addr,
		ServiceName: "smoke-svc",
		MaxConns:    1,
	}); err != nil {
		t.Fatalf("logtrack init: %v", err)
	}
	defer logtrack.Close()

	logtrack.InboundHTTP(&logtrack.InboundHTTPLog{
		AppID:          123,
		Country:        "id",
		BID:            "smoke",
		Method:         "GET",
		URL:            "/api/test",
		ResponseStatus: 200,
		DurationMs:     12,
	})

	logtrack.Send("custom-topic", map[string]any{
		"hello": "world",
	}, logtrack.WithTraceID("trace-001"))

	// Wait for the messages to traverse: SDK → server → queue → handler → fallback.
	time.Sleep(800 * time.Millisecond)

	// Flush manager so the active .log rotates to .log.done for the assertion.
	flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	mgr.Shutdown(flushCtx)
	cancel()

	matches, _ := filepath.Glob(filepath.Join(tmp, "*.log.done"))
	if len(matches) == 0 {
		// fallback writer may still hold an unrolled file.
		extras, _ := filepath.Glob(filepath.Join(tmp, "*.log"))
		matches = append(matches, extras...)
	}
	if len(matches) == 0 {
		t.Fatalf("no fallback files found in %s", tmp)
	}

	gotInbound, gotCustom := false, false
	for _, p := range matches {
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		sc := bufio.NewScanner(f)
		buf := make([]byte, 0, 1<<16)
		sc.Buffer(buf, 16*1024*1024)
		for sc.Scan() {
			var env envelope.Envelope
			if err := json.Unmarshal(sc.Bytes(), &env); err != nil {
				continue
			}
			if env.Topic == envelope.TopicInboundHTTPLogs {
				gotInbound = true
			}
			if env.Topic == "custom-topic" {
				gotCustom = true
				if !strings.Contains(string(env.Data), `"hello":"world"`) {
					t.Errorf("custom-topic data unexpected: %s", env.Data)
				}
			}
		}
		_ = f.Close()
	}
	if !gotInbound {
		t.Errorf("inbound-http-logs envelope not found in fallback")
	}
	if !gotCustom {
		t.Errorf("custom-topic envelope not found in fallback")
	}
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server never came up on %s", addr)
}
