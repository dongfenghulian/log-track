package rpc

import (
	"encoding/json"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicRPCCalls)
	if !ok || h.Topic() != envelope.TopicRPCCalls {
		t.Errorf("not registered")
	}
}

func TestHandle_RequiresCallerCalleeMethod(t *testing.T) {
	cases := []map[string]any{
		{"callee": "b", "method": "/m"},
		{"caller": "a", "method": "/m"},
		{"caller": "a", "callee": "b"},
	}
	for i, body := range cases {
		raw, _ := json.Marshal(body)
		if err := (&Handler{}).Handle(&envelope.Envelope{Data: raw}); err == nil {
			t.Errorf("case %d should error: %v", i, body)
		}
	}
}

func TestHandle_AcceptsValid(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"caller": "a", "callee": "b", "method": "/m"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err != nil {
		t.Errorf("valid: %v", err)
	}
}
