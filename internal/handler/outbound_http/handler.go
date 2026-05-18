// Package outbound_http handles the outbound-http-logs topic.
package outbound_http

import (
	"encoding/json"
	"errors"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicOutboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicOutboundHTTPLogs }

type payload struct {
	Provider       string `json:"provider"`
	Method         string `json:"method"`
	URL            string `json:"url"`
	ResponseStatus int    `json:"response_status"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.Provider == "" {
		return errors.New("outbound-http-logs: provider is required")
	}
	if p.Method == "" || p.URL == "" {
		return errors.New("outbound-http-logs: method and url are required")
	}
	if p.ResponseStatus == 0 {
		return errors.New("outbound-http-logs: response_status is required")
	}
	writeradapter.Write(env)
	return nil
}
