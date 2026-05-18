// Package writeradapter bridges handler subpackages to the runtime writer.Manager.
//
// Handlers register at init() time, before main wires up Kafka/fallback. They can't import
// internal/writer directly (circular: handler → writer → ... → handler via gateway main),
// nor can they receive the manager via Register call (we want zero-arg init() registrations).
//
// So: main calls Set with a function that ultimately reaches manager.Write. Handlers call Write.
package writeradapter

import (
	"sync/atomic"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type WriteFunc func(*envelope.Envelope)

var fn atomic.Pointer[WriteFunc]

// Set installs the dispatch function. Call once from main after the writer.Manager is constructed.
func Set(f WriteFunc) {
	fn.Store(&f)
}

// Write hands the envelope to whatever Set installed. Drops if Set has not run yet (shouldn't happen in practice).
func Write(env *envelope.Envelope) {
	p := fn.Load()
	if p == nil {
		return
	}
	(*p)(env)
}
