package event

import (
	"encoding/json"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicEventTracks)
	if !ok || h.Topic() != envelope.TopicEventTracks {
		t.Errorf("not registered")
	}
}

func TestHandle_RequiresBID(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"event_name": "x", "platform": "android", "app_version": "1.0"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing bid should error")
	}
}

func TestHandle_RequiresEventName(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"bid": "x", "platform": "android", "app_version": "1.0"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing event_name should error")
	}
}

func TestHandle_RequiresPlatformAndVersion(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"bid": "x", "event_name": "x"})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err == nil {
		t.Errorf("missing platform/version should error")
	}
}

func TestHandle_AcceptsValid(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"bid": "mx01", "event_name": "loan.apply_submitted",
		"platform": "android", "app_version": "1.0",
	})
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: body}); err != nil {
		t.Errorf("valid payload rejected: %v", err)
	}
}
