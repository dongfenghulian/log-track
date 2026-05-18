// Package router dispatches envelopes to the registered handler for their topic.
//
// Built-in handlers register themselves via init() in their respective packages.
// The aggregate package internal/handler imports them to trigger registration.
package router

import (
	"sync"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// Handler validates an envelope and forwards it to the writer.
// Implementations are typically created once at startup and shared across workers, so they must be safe for concurrent use.
type Handler interface {
	Topic() string
	Handle(env *envelope.Envelope) error
}

var (
	mu       sync.RWMutex
	registry = map[string]Handler{}
)

// Register adds a handler. Panics on duplicate topic — duplicate registration is a programming error.
func Register(topic string, h Handler) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[topic]; exists {
		panic("logtrack: duplicate handler for topic: " + topic)
	}
	registry[topic] = h
}

// Lookup returns the handler for a topic, if any.
func Lookup(topic string) (Handler, bool) {
	mu.RLock()
	defer mu.RUnlock()
	h, ok := registry[topic]
	return h, ok
}
