// Package sys_message handles the sys.sys-event-v1 topic.
package sys_message

import (
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicSysEvent, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicSysEvent }

func (h *Handler) Handle(env *envelope.Envelope) error {
	env.WriteRaw = true
	writeradapter.Write(env)
	return nil
}
