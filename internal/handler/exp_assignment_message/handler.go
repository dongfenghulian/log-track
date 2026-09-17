// Package exp_assignment_message handles the dw.exp-assignment-v1 topic.
package exp_assignment_message

import (
	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicExpAssignment, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicExpAssignment }

func (h *Handler) Handle(env *envelope.Envelope) error {
	env.WriteRaw = true
	writeradapter.Write(env)
	return nil
}
