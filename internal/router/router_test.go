package router

import (
	"sync"
	"testing"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type stubHandler struct{ topic string }

func (s *stubHandler) Topic() string                            { return s.topic }
func (s *stubHandler) Handle(*envelope.Envelope) error          { return nil }

// Tests use unique topic names so they don't collide with handlers registered elsewhere
// (built-in handlers register via their own init when those packages are imported, but this
// test package does not import them, so the registry is empty here unless someone else fills it).

func TestRegisterAndLookup(t *testing.T) {
	const topic = "router-test-topic-1"
	h := &stubHandler{topic: topic}
	Register(topic, h)
	t.Cleanup(func() { unregisterForTest(topic) })

	got, ok := Lookup(topic)
	if !ok || got != h {
		t.Errorf("lookup got=%v ok=%v", got, ok)
	}
}

func TestLookup_Missing(t *testing.T) {
	if _, ok := Lookup("router-test-nonexistent-topic"); ok {
		t.Errorf("expected not-found")
	}
}

func TestRegister_DuplicatePanics(t *testing.T) {
	const topic = "router-test-duplicate"
	Register(topic, &stubHandler{topic: topic})
	t.Cleanup(func() { unregisterForTest(topic) })

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("duplicate register should panic")
		}
	}()
	Register(topic, &stubHandler{topic: topic})
}

func TestRegister_ConcurrentSafe(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t := "router-test-concurrent-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
			Register(t, &stubHandler{topic: t})
			defer unregisterForTest(t)
			Lookup(t)
		}(i)
	}
	wg.Wait()
}

// unregisterForTest is a tests-only helper that bypasses the panic-on-duplicate check.
// We intentionally do not export this from the package — registration is meant to be one-shot at startup.
func unregisterForTest(topic string) {
	mu.Lock()
	delete(registry, topic)
	mu.Unlock()
}
