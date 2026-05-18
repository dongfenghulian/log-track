package passthrough

import (
	"sync/atomic"
	"testing"

	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestPassthrough_WritesEnvelopeAsIs(t *testing.T) {
	var seen atomic.Pointer[envelope.Envelope]
	writeradapter.Set(func(env *envelope.Envelope) {
		seen.Store(env)
	})
	defer writeradapter.Set(func(*envelope.Envelope) {})

	in := &envelope.Envelope{
		Version: envelope.Version,
		Topic:   "custom-anything",
		Service: "svc",
		Data:    []byte(`{"weird": true}`),
	}
	if err := (&Handler{}).Handle(in); err != nil {
		t.Fatal(err)
	}
	got := seen.Load()
	if got == nil {
		t.Fatal("writer not called")
	}
	if got.Topic != "custom-anything" {
		t.Errorf("topic preserved? got %q", got.Topic)
	}
}
