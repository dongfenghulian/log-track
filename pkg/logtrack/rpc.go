package logtrack

import (
	"context"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// RPCLog is the data payload for topic rpc-calls.
type RPCLog struct {
	AppID        int            `json:"app_id,omitempty"`
	Country      string         `json:"country,omitempty"`
	BID          string         `json:"bid,omitempty"`
	Caller       string         `json:"caller"`
	Callee       string         `json:"callee"`
	Method       string         `json:"method"`
	DurationMs   int64          `json:"duration_ms"`
	StatusCode   int            `json:"status_code"`
	Error        string         `json:"error,omitempty"`
	RequestSize  int64          `json:"request_size,omitempty"`
	ResponseSize int64          `json:"response_size,omitempty"`
	Extra        map[string]any `json:"extra,omitempty"`
}

// RPC sends an RPC call log.
func RPC(l *RPCLog, opts ...Option) {
	if l == nil {
		return
	}
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicRPCCalls, l, o.traceID, o.partitionKey)
	}
}

// RPCCtx is the ctx-aware variant.
func RPCCtx(ctx context.Context, l *RPCLog, opts ...Option) {
	if l == nil {
		return
	}
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	if o.partitionKey == "" {
		o.partitionKey = partitionKeyFromCtx(ctx)
	}
	if c := client(); c != nil {
		c.send(envelope.TopicRPCCalls, l, o.traceID, o.partitionKey)
	}
}
