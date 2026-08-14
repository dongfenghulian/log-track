// Package inbound_http handles the inbound-http-logs topic.
//
// Validation here is intentionally light: the SDK is the source of truth for shape;
// gateway only checks fields that downstream consumers depend on existing.
package inbound_http

import (
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicInboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicInboundHTTPLogs }

func (h *Handler) Handle(env *envelope.Envelope) error {
	writeradapter.Write(env)
	return nil
}
