package outbound_http

import (
	"encoding/json"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicOutboundHTTPLogs)
	if !ok || h.Topic() != envelope.TopicOutboundHTTPLogs {
		t.Errorf("not registered")
	}
}

func TestHandle_RequiresProvider(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"method": "POST", "url": "https://x", "response_status": 200})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing provider should error")
	}
}

func TestHandle_AcceptsValid(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"provider": "stripe", "method": "POST", "url": "https://x", "response_status": 200})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err != nil {
		t.Errorf("valid payload rejected: %v", err)
	}
}
