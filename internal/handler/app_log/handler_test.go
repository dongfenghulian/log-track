package app_log

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestInit_RegistersHandler(t *testing.T) {
	h, ok := router.Lookup(envelope.TopicAppLogs)
	if !ok || h.Topic() != envelope.TopicAppLogs {
		t.Errorf("not registered")
	}
}

// withCapture installs a writeradapter sink for the duration of fn and returns the captured envelopes.
// Tests share the writeradapter package state, so we serialize them.
var captureMu sync.Mutex

func withCapture(t *testing.T, fn func()) []*envelope.Envelope {
	t.Helper()
	captureMu.Lock()
	defer captureMu.Unlock()

	var got []*envelope.Envelope
	writeradapter.Set(func(env *envelope.Envelope) {
		got = append(got, env)
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})
	fn()
	return got
}

func TestHandle_LevelRoutesToPerLevelTopic(t *testing.T) {
	cases := []struct {
		level     string
		wantTopic string
	}{
		{"ERROR", envelope.TopicAppLogsError},
		{"WARN", envelope.TopicAppLogsWarn},
		{"INFO", envelope.TopicAppLogsInfo},
		{"DEBUG", envelope.TopicAppLogsDebug},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.level, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"level": tc.level, "message": "hello"})
			envs := withCapture(t, func() {
				err := (&Handler{}).Handle(&envelope.Envelope{
					Topic: envelope.TopicAppLogs,
					Data:  body,
				})
				if err != nil {
					t.Errorf("handle: %v", err)
				}
			})
			if len(envs) != 1 {
				t.Fatalf("expected 1 envelope written, got %d", len(envs))
			}
			if envs[0].Topic != tc.wantTopic {
				t.Errorf("topic=%q want %q", envs[0].Topic, tc.wantTopic)
			}
		})
	}
}

func TestHandle_RejectsUnknownLevel(t *testing.T) {
	for _, bad := range []string{"TRACE", "error", "Info", "FATAL", "X"} {
		body, _ := json.Marshal(map[string]any{"level": bad, "message": "hi"})
		err := (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicAppLogs, Data: body})
		if err == nil {
			t.Errorf("level %q should be rejected", bad)
		}
	}
}

func TestHandle_RejectsUnknownLevel_DoesNotForwardToWriter(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"level": "TRACE", "message": "hi"})
	envs := withCapture(t, func() {
		_ = (&Handler{}).Handle(&envelope.Envelope{Topic: envelope.TopicAppLogs, Data: body})
	})
	if len(envs) != 0 {
		t.Errorf("rejected envelope must not reach writer, got %d", len(envs))
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

func TestHandle_RejectsMalformedJSON(t *testing.T) {
	if err := (&Handler{}).Handle(&envelope.Envelope{Data: []byte("not json")}); err == nil {
		t.Errorf("malformed json should error")
	}
}
