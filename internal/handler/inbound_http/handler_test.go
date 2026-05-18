package inbound_http

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicInboundHTTPLogs)
	if !ok {
		t.Fatal("inbound-http-logs handler not registered")
	}
	if h.Topic() != envelope.TopicInboundHTTPLogs {
		t.Errorf("topic mismatch: %q", h.Topic())
	}
}

func TestHandle_RejectsMissingMethod(t *testing.T) {
	body := mustMarshal(map[string]any{"url": "/x", "response_status": 200})
	err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: body})
	if err == nil {
		t.Errorf("missing method should error")
	}
}

func TestHandle_RejectsMissingURL(t *testing.T) {
	body := mustMarshal(map[string]any{"method": "GET", "response_status": 200})
	err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: body})
	if err == nil {
		t.Errorf("missing url should error")
	}
}

func TestHandle_RejectsMissingStatus(t *testing.T) {
	body := mustMarshal(map[string]any{"method": "GET", "url": "/x"})
	err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: body})
	if err == nil {
		t.Errorf("missing response_status should error")
	}
}

func TestHandle_PassesValidEnvelopeToWriter(t *testing.T) {
	var written atomic.Int32
	writeradapter.Set(func(env *envelope.Envelope) {
		if env.Topic == envelope.TopicInboundHTTPLogs {
			written.Add(1)
		}
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})

	body := mustMarshal(map[string]any{"method": "GET", "url": "/x", "response_status": 200})
	err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: body})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if written.Load() != 1 {
		t.Errorf("expected 1 write, got %d", written.Load())
	}
}

func TestHandle_RejectsMalformedJSON(t *testing.T) {
	err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: []byte("not json")})
	if err == nil {
		t.Errorf("malformed JSON should error")
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
