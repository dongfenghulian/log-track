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

func TestHandle_PassesEnvelopeToWriterWithoutBusinessValidation(t *testing.T) {
	var written atomic.Int32
	writeradapter.Set(func(env *envelope.Envelope) {
		if env.Topic == envelope.TopicInboundHTTPLogs {
			written.Add(1)
		}
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})

	cases := [][]byte{
		mustMarshal(map[string]any{"method": "GET", "url": "/x", "response_status": 200}),
		mustMarshal(map[string]any{"url": "/x", "response_status": 200}),
		mustMarshal(map[string]any{"method": "GET", "response_status": 200}),
		mustMarshal(map[string]any{"method": "GET", "url": "/x"}),
		[]byte("not json"),
	}
	for _, body := range cases {
		err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicInboundHTTPLogs, Data: body})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if written.Load() != int32(len(cases)) {
		t.Errorf("expected %d writes, got %d", len(cases), written.Load())
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
