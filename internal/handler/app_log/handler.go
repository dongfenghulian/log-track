// Package app_log handles the app-logs topic.
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

var validLevels = map[string]struct{}{
	"DEBUG": {}, "INFO": {}, "WARN": {}, "ERROR": {},
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.Level == "" || p.Message == "" {
		return errors.New("app-logs: level and message are required")
	}
	if _, ok := validLevels[p.Level]; !ok {
		return errors.New("app-logs: invalid level " + p.Level)
	}
	writeradapter.Write(env)
	return nil
}
