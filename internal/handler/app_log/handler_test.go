package app_log

import (
	"encoding/json"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicAppLogs)
	if !ok || h.Topic() != envelope.TopicAppLogs {
		t.Errorf("not registered")
	}
}

func TestHandle_AcceptsAllValidLevels(t *testing.T) {
	for _, lvl := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
		body, _ := json.Marshal(map[string]any{"level": lvl, "message": "hello"})
		if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err != nil {
			t.Errorf("level %s rejected: %v", lvl, err)
		}
	}
}

func TestHandle_RejectsUnknownLevel(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"level": "TRACE", "message": "hi"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("unknown level should error")
	}
}

func TestHandle_RequiresLevelAndMessage(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"message": "hi"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing level should error")
	}
	body, _ = json.Marshal(map[string]any{"level": "INFO"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing message should error")
	}
}
