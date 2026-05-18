package writeradapter

import (
	"sync/atomic"
	"testing"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestWrite_BeforeSet_IsNoop(t *testing.T) {
	// Reset to nil for the test (fresh package state in tests is not guaranteed across tests in
	// the same package, so reset explicitly).
	fn.Store(nil)
	Write(&envelope.Envelope{Topic: "anything"}) // must not panic
}

func TestSet_AndDispatch(t *testing.T) {
	var calls atomic.Int32
	var lastTopic atomic.Pointer[string]
	Set(func(e *envelope.Envelope) {
		calls.Add(1)
		t := e.Topic
		lastTopic.Store(&t)
	})
	defer fn.Store(nil)

	Write(&envelope.Envelope{Topic: "abc"})
	if calls.Load() != 1 {
		t.Errorf("calls=%d", calls.Load())
	}
	if got := lastTopic.Load(); got == nil || *got != "abc" {
		t.Errorf("topic mismatch: %v", got)
	}
}

func TestSet_Replaces(t *testing.T) {
	var first, second atomic.Int32
	Set(func(*envelope.Envelope) { first.Add(1) })
	Set(func(*envelope.Envelope) { second.Add(1) })
	defer fn.Store(nil)

	Write(&envelope.Envelope{})
	if first.Load() != 0 || second.Load() != 1 {
		t.Errorf("first=%d second=%d", first.Load(), second.Load())
	}
}
