package queue

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestQueue_DispatchesAllEnqueuedToWorkers(t *testing.T) {
	q := New(100)
	var seen atomic.Int32
	q.Start(4, func(env *envelope.Envelope) {
		if env.Topic == "x" {
			seen.Add(1)
		}
	})
	for i := 0; i < 50; i++ {
		q.Enqueue(&envelope.Envelope{Topic: "x"})
	}
	q.Close()
	if got := seen.Load(); got != 50 {
		t.Errorf("processed %d, want 50", got)
	}
}

func TestQueue_EnqueueBlocksWhenFull(t *testing.T) {
	q := New(2)
	// Fill without starting workers; the third enqueue must block.
	q.Enqueue(&envelope.Envelope{})
	q.Enqueue(&envelope.Envelope{})

	done := make(chan struct{})
	go func() {
		q.Enqueue(&envelope.Envelope{})
		close(done)
	}()

	select {
	case <-done:
		t.Errorf("third enqueue should have blocked, but returned immediately")
	case <-time.After(100 * time.Millisecond):
	}

	// Drain: start a worker; the blocked enqueue should release.
	q.Start(1, func(*envelope.Envelope) {})
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Errorf("blocked enqueue never released")
	}
	q.Close()
}

func TestQueue_LenReportsDepth(t *testing.T) {
	q := New(10)
	if q.Len() != 0 {
		t.Errorf("empty len=%d", q.Len())
	}
	q.Enqueue(&envelope.Envelope{})
	q.Enqueue(&envelope.Envelope{})
	if q.Len() != 2 {
		t.Errorf("after 2 enqueues len=%d", q.Len())
	}
	// Drain.
	q.Start(1, func(*envelope.Envelope) {})
	q.Close()
}
