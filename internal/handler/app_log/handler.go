// Package app_log handles the app-logs topic.
//
// The SDK sends envelopes with topic="app-logs"; this handler validates the level field
// and rewrites env.Topic to one of:
//   ERROR → app-logs-error
//   WARN  → app-logs-warn
//   INFO  → app-logs-info
//   DEBUG → app-logs-debug
// before forwarding to the writer. Unknown levels are rejected (strict mode).
package app_log

import (
	"encoding/json"
	"errors"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicAppLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicAppLogs }

type payload struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

// levelTopic maps a normalized level string to the Kafka topic that should receive the log.
var levelTopic = map[string]string{
	"ERROR": envelope.TopicAppLogsError,
	"WARN":  envelope.TopicAppLogsWarn,
	"INFO":  envelope.TopicAppLogsInfo,
	"DEBUG": envelope.TopicAppLogsDebug,
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.Level == "" || p.Message == "" {
		return errors.New("app-logs: level and message are required")
	}
	target, ok := levelTopic[p.Level]
	if !ok {
		return errors.New("app-logs: invalid level " + p.Level)
	}
	env.Topic = target
	writeradapter.Write(env)
	return nil
}
