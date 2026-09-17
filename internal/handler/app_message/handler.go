// Package app_message handles the app.app-event-v1 topic.
package app_message

import (
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicAppEvent, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicAppEvent }

func (h *Handler) Handle(env *envelope.Envelope) error {
	env.WriteRaw = true
	writeradapter.Write(env)
	return nil
}
