// Package rpc handles the rpc-calls topic.
package rpc

import (
	"encoding/json"
	"errors"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicRPCCalls, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicRPCCalls }

type payload struct {
	Caller string `json:"caller"`
	Callee string `json:"callee"`
	Method string `json:"method"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.Caller == "" || p.Callee == "" || p.Method == "" {
		return errors.New("rpc-calls: caller/callee/method are required")
	}
	writeradapter.Write(env)
	return nil
}
