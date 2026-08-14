// Package queue is a fixed-capacity in-memory channel with a worker pool.
package queue

import (
	"sync"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// Queue buffers incoming envelopes and dispatches them to workers.
type Queue struct {
	name       string
	ch         chan *envelope.Envelope
	closeOnce  sync.Once
	closed     bool
	done       chan struct{}
	producerMu sync.Mutex
	producers  sync.WaitGroup
	wg         sync.WaitGroup
}

// New creates a queue with the given capacity.
func New(capacity int) *Queue {
	return NewNamed("default", capacity)
}

// NewNamed creates a named queue with the given capacity.
func NewNamed(name string, capacity int) *Queue {
	if name == "" {
		name = "default"
	}
	return &Queue{
		name: name,
		ch:   make(chan *envelope.Envelope, capacity),
		done: make(chan struct{}),
	}
}

// Enqueue blocks until the envelope is accepted or the queue is closed.
// Server-side drops are not implemented here; backpressure is delegated to the TCP layer
// (slow consumers slow down their TCP read loop, which slows down clients on shared connections).
func (q *Queue) Enqueue(env *envelope.Envelope) bool {
	q.producerMu.Lock()
	if q.closed {
		q.producerMu.Unlock()
		return false
	}
	q.producers.Add(1)
	q.producerMu.Unlock()
	defer q.producers.Done()

	select {
	case <-q.done:
		return false
	case q.ch <- env:
		q.reportDepth()
		return true
	}
}

// Start spawns n workers calling fn for each envelope.
func (q *Queue) Start(n int, fn func(*envelope.Envelope)) {
	for i := 0; i < n; i++ {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			for {
				select {
				case env := <-q.ch:
					fn(env)
					q.reportDepth()
				case <-q.done:
					q.producers.Wait()
					for {
						select {
						case env := <-q.ch:
							fn(env)
							q.reportDepth()
						default:
							return
						}
					}
				}
			}
		}()
	}
}

// StopEnqueue releases blocked producers and makes future Enqueue calls return false.
func (q *Queue) StopEnqueue() {
	q.closeOnce.Do(func() {
		q.producerMu.Lock()
		q.closed = true
		close(q.done)
		q.producerMu.Unlock()
	})
	q.producers.Wait()
}

// Close signals workers to exit after draining and blocks until they do.
func (q *Queue) Close() {
	q.StopEnqueue()
	q.wg.Wait()
}

// Len returns the current queue depth.
func (q *Queue) Len() int { return len(q.ch) }

func (q *Queue) reportDepth() {
	metrics.QueueDepthSetForQueue(q.name, len(q.ch))
}
