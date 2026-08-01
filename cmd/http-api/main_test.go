package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dongfenghulian/log-track/internal/config"
)

func TestHTTPAPIHandler_WritesRawBody(t *testing.T) {
	var gotTopic string
	var gotBody []byte

	h := httpAPIHandler(
		config.Config{
			Server: config.ServerConfig{MaxMessageSize: 1024},
			Kafka:  config.KafkaConfig{},
		},
		func(ctx context.Context, cfg config.KafkaConfig, topic string, payload []byte) error {
			gotTopic = topic
			gotBody = append([]byte(nil), payload...)
			return nil
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	req := httptest.NewRequest(http.MethodPost, "/http-api?topic=test-topic", bytes.NewBufferString(`{"x":1}`))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status=%d want=%d", rr.Code, http.StatusNoContent)
	}
	if gotTopic != "test-topic" {
		t.Fatalf("topic=%q want=%q", gotTopic, "test-topic")
	}
	if string(gotBody) != `{"x":1}` {
		t.Fatalf("body=%q want=%q", string(gotBody), `{"x":1}`)
	}
}

func TestHTTPAPIHandler_RejectsMissingTopic(t *testing.T) {
	h := httpAPIHandler(
		config.Config{Server: config.ServerConfig{MaxMessageSize: 1024}},
		func(ctx context.Context, cfg config.KafkaConfig, topic string, payload []byte) error {
			t.Fatal("writer should not be called")
			return nil
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	req := httptest.NewRequest(http.MethodPost, "/http-api", bytes.NewBufferString(`{"x":1}`))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rr.Code, http.StatusBadRequest)
	}
}

func TestHTTPAPIHandler_RejectsNonPost(t *testing.T) {
	h := httpAPIHandler(
		config.Config{Server: config.ServerConfig{MaxMessageSize: 1024}},
		func(ctx context.Context, cfg config.KafkaConfig, topic string, payload []byte) error {
			t.Fatal("writer should not be called")
			return nil
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	req := httptest.NewRequest(http.MethodGet, "/http-api?topic=test-topic", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=%d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestDefaultHTTPAddr(t *testing.T) {
	if got := defaultHTTPAddr(); got != ":9591" {
		t.Fatalf("got %q want %q", got, ":9591")
	}
}
