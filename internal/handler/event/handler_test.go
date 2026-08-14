package event

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicEventTracks)
	if !ok || h.Topic() != envelope.TopicEventTracks {
		t.Errorf("not registered")
	}
}

func TestHandle_PassesEnvelopeToWriterWithoutBusinessValidation(t *testing.T) {
	var written atomic.Int32
	writeradapter.Set(func(env *envelope.Envelope) {
		if env.Topic == envelope.TopicEventTracks {
			written.Add(1)
		}
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})

	valid, _ := json.Marshal(map[string]any{
		"bid": "mx01", "event_name": "loan.apply_submitted",
		"platform": "android", "app_version": "1.0",
	})
	missingBid, _ := json.Marshal(map[string]any{"event_name": "x", "platform": "android", "app_version": "1.0"})
	missingEventName, _ := json.Marshal(map[string]any{"bid": "x", "platform": "android", "app_version": "1.0"})
	missingPlatformVersion, _ := json.Marshal(map[string]any{"bid": "x", "event_name": "x"})

	cases := [][]byte{
		valid,
		missingBid,
		missingEventName,
		missingPlatformVersion,
		[]byte("not json"),
	}
	for _, body := range cases {
		if err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicEventTracks, Data: body}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if written.Load() != int32(len(cases)) {
		t.Errorf("expected %d writes, got %d", len(cases), written.Load())
	}
}
