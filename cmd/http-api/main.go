package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/dongfenghulian/log-track/internal/config"
	"github.com/segmentio/kafka-go"
)

type topicWriter func(context.Context, config.KafkaConfig, string, []byte) error

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "", "http listen address (default: :9591)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()
	if addr == "" {
		addr = defaultHTTPAddr()
	}
	logger.Info("http api starting",
		"addr", addr,
		"brokers", cfg.Kafka.Brokers,
		"body_limit", cfg.Server.MaxMessageSize)

	mux := http.NewServeMux()
	mux.HandleFunc("/http-api", httpAPIHandler(cfg, writeRawKafka, logger))

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("http api listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http api server error", "err", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Shutdown.Timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Warn("http api shutdown error", "err", err)
	}
	logger.Info("http api shutdown complete")
}

func httpAPIHandler(cfg config.Config, write topicWriter, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "missing topic", http.StatusBadRequest)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, int64(cfg.Server.MaxMessageSize))
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
			return
		}

		if err := write(r.Context(), cfg.Kafka, topic, body); err != nil {
			logger.Error("kafka write failed", "topic", topic, "err", err)
			http.Error(w, "kafka write failed", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func writeRawKafka(ctx context.Context, cfg config.KafkaConfig, topic string, payload []byte) error {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		WriteTimeout: cfg.WriteTimeout,
	}
	defer w.Close()

	return w.WriteMessages(ctx, kafka.Message{Value: payload})
}

func defaultHTTPAddr() string {
	return ":9591"
}
