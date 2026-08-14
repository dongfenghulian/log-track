package outbound_http

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicOutboundHTTPLogs)
	if !ok || h.Topic() != envelope.TopicOutboundHTTPLogs {
		t.Errorf("not registered")
	}
}

func TestHandle_PassesEnvelopeToWriterWithoutBusinessValidation(t *testing.T) {
	var written atomic.Int32
	writeradapter.Set(func(env *envelope.Envelope) {
		if env.Topic == envelope.TopicOutboundHTTPLogs {
			written.Add(1)
		}
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})

	valid, _ := json.Marshal(map[string]any{"provider": "stripe", "method": "POST", "url": "https://x", "response_status": 200})
	missingProvider, _ := json.Marshal(map[string]any{"method": "POST", "url": "https://x", "response_status": 200})

	cases := [][]byte{
		valid,
		missingProvider,
		[]byte("not json"),
	}
	for _, body := range cases {
		if err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicOutboundHTTPLogs, Data: body}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if written.Load() != int32(len(cases)) {
		t.Errorf("expected %d writes, got %d", len(cases), written.Load())
	}
}
