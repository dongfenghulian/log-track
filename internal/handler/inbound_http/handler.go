// Package inbound_http handles the inbound-http-logs topic.
//
// Validation here is intentionally light: the SDK is the source of truth for shape;
// gateway only checks fields that downstream consumers depend on existing.
package inbound_http

import (
	"encoding/json"
	"errors"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicInboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicInboundHTTPLogs }

type payload struct {
	Method         string `json:"method"`
	URL            string `json:"url"`
	ResponseStatus int    `json:"response_status"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.Method == "" || p.URL == "" {
		return errors.New("inbound-http-logs: method and url are required")
	}
	if p.ResponseStatus == 0 {
		return errors.New("inbound-http-logs: response_status is required")
	}
	writeradapter.Write(env)
	return nil
}
