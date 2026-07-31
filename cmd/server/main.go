// Command gateway is the LogTrack server.
//
// It listens on a TCP port for length-prefixed JSON envelopes (see PROTOCOL.md), routes them
// by topic to a registered handler (or a passthrough for unknown topics), and writes the result
// to Kafka with on-disk fallback when Kafka is unhealthy.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dongfenghulian/log-track/internal/config"
	"github.com/dongfenghulian/log-track/internal/handler/passthrough"
	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/internal/queue"
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/server"
	"github.com/dongfenghulian/log-track/internal/version"
	"github.com/dongfenghulian/log-track/internal/writer"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"

	// blank import triggers init() registration of all built-in handlers.
	_ "github.com/dongfenghulian/log-track/internal/handler"
)

func main() {
	var showVersion bool
	flag.BoolVar(&showVersion, "v", false, "print build version information and exit")
	flag.BoolVar(&showVersion, "version", false, "print build version information and exit")
	flag.Parse()
	if showVersion {
		fmt.Println(version.StringWithAppEnv(appEnv()))
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()
	logger.Info("config loaded",
		"address", cfg.Server.Address,
		"metrics_address", cfg.Server.MetricsAddress,
		"brokers", cfg.Kafka.Brokers,
		"max_connections", cfg.Server.MaxConnections,
		"queue_capacity", cfg.Server.QueueSize,
		"worker_goroutines", cfg.Server.WorkerCount)

	// Wire writers.
	kafkaWriter := writer.NewKafkaWriter(cfg.Kafka.Brokers, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout, cfg.Kafka.WriteTimeout)
	fallbackWriter, err := writer.NewFallbackWriter(cfg.Fallback.DataDir, cfg.Fallback.MaxFileSize, cfg.Fallback.MaxFiles)
	if err != nil {
		logger.Error("failed to init fallback writer", "err", err)
		os.Exit(1)
	}
	manager := writer.NewManager(kafkaWriter, fallbackWriter, cfg.Shutdown.KafkaFlushTimeout, logger)
	writeradapter.Set(manager.Write)

	// Wire queue + workers.
	pass := &passthrough.Handler{}
	q := queue.New(cfg.Server.QueueSize)
	q.Start(cfg.Server.WorkerCount, func(env *envelope.Envelope) {
		h, ok := router.Lookup(env.Topic)
		if !ok {
			if err := pass.Handle(env); err != nil {
				logger.Warn("passthrough error", "topic", env.Topic, "err", err)
				metrics.MessageObserved(env.Topic, "invalid")
				return
			}
			metrics.MessageObserved(env.Topic, "passthrough")
			return
		}
		if err := h.Handle(env); err != nil {
			logger.Warn("handler error", "topic", env.Topic, "err", err)
			metrics.MessageObserved(env.Topic, "invalid")
			return
		}
		metrics.MessageObserved(env.Topic, "handled")
	})

	// Wire server.
	srv := server.New(server.Config{
		Address:         cfg.Server.Address,
		MaxConnections:  cfg.Server.MaxConnections,
		MaxMessageSize:  cfg.Server.MaxMessageSize,
		ConnReadTimeout: cfg.Shutdown.ConnReadTimeout,
	}, q, logger)

	go func() {
		if err := srv.Start(); err != nil {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Metrics HTTP server. Exposes /metrics on a separate port so it survives gateway shutdown
	// for at least the scrape interval (still returns the final counters).
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{Addr: cfg.Server.MetricsAddress, Handler: metricsMux}
	go func() {
		logger.Info("metrics listening", "addr", cfg.Server.MetricsAddress)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "err", err)
		}
	}()

	// Wait for SIGTERM/SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	logger.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown.Timeout)
	defer cancel()

	// Order: stop accepting new conns → drain existing → drain queue → flush kafka → close fallback.
	srv.Shutdown(shutdownCtx)
	q.Close()

	// Manager.Shutdown handles the kafka flush + fallback close, bounded by remaining time in shutdownCtx.
	manager.Shutdown(shutdownCtx)

	// Stop metrics server last so a final scrape can still pick up counters.
	_ = metricsServer.Shutdown(shutdownCtx)

	// Spend any leftover budget so logs flush before exit.
	deadline, _ := shutdownCtx.Deadline()
	if remaining := time.Until(deadline); remaining > 0 && remaining < 100*time.Millisecond {
		time.Sleep(remaining)
	}
	logger.Info("shutdown complete")
}

func appEnv() string {
	if v := os.Getenv("APP_ENV"); v != "" {
		return v
	}
	return "dev"
}
