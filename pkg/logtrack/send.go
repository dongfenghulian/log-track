package logtrack

import "context"

// Send delivers data to an arbitrary topic. Use this for custom topics; helpers below cover built-ins.
func Send(topic string, data any, opts ...Option) {
	c := client()
	if c == nil {
		return
	}
	o := applyOpts(opts)
	c.send(topic, data, o.traceID, o.partitionKey)
}

// SendCtx is the ctx-aware variant of Send. trace_id and partition_key are read from ctx
// (CtxWithTraceID / CtxWithPartitionKey) when not provided via options.
func SendCtx(ctx context.Context, topic string, data any, opts ...Option) {
	c := client()
	if c == nil {
		return
	}
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	if o.partitionKey == "" {
		o.partitionKey = partitionKeyFromCtx(ctx)
	}
	c.send(topic, data, o.traceID, o.partitionKey)
}
