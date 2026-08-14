// Package outbound_http handles the outbound-http-logs topic.
package outbound_http

import (
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicOutboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicOutboundHTTPLogs }

func (h *Handler) Handle(env *envelope.Envelope) error {
	writeradapter.Write(env)
	return nil
}
