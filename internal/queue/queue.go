// Package queue is a fixed-capacity in-memory channel with a worker pool.
package queue

import (
	"sync"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// Queue buffers incoming envelopes and dispatches them to workers.
type Queue struct {
	ch chan *envelope.Envelope
	wg sync.WaitGroup
}

// New creates a queue with the given capacity.
func New(capacity int) *Queue {
	return &Queue{ch: make(chan *envelope.Envelope, capacity)}
}

// Enqueue blocks until the envelope is accepted (or the channel is closed).
// Server-side drops are not implemented here; backpressure is delegated to the TCP layer
// (slow consumers slow down their TCP read loop, which slows down clients on shared connections).
func (q *Queue) Enqueue(env *envelope.Envelope) {
	q.ch <- env
	metrics.QueueDepthSet(len(q.ch))
}

// Start spawns n workers calling fn for each envelope.
func (q *Queue) Start(n int, fn func(*envelope.Envelope)) {
	for i := 0; i < n; i++ {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			for env := range q.ch {
				fn(env)
				metrics.QueueDepthSet(len(q.ch))
			}
		}()
	}
}

// Close signals workers to exit after draining and blocks until they do.
func (q *Queue) Close() {
	close(q.ch)
	q.wg.Wait()
}

// Len returns the current queue depth.
func (q *Queue) Len() int { return len(q.ch) }
