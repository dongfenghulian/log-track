// Package passthrough writes any envelope as-is to its declared topic.
//
// Unlike the other handler packages, passthrough does NOT register itself in router;
// the router's "topic not found" path uses it directly via Handle.
package passthrough

import (
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type Handler struct{}

func (h *Handler) Handle(env *envelope.Envelope) error {
	writeradapter.Write(env)
	return nil
}
