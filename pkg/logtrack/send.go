package logtrack

import "context"

// Send delivers data to an arbitrary topic. Use this for custom topics; helpers below cover built-ins.
func Send(topic string, data any, opts ...Option) {
	c := client()
	if c == nil {
		return
	}
	o := applyOpts(opts)
	c.send(topic, data, o.traceID)
}

// SendCtx is the ctx-aware variant of Send. trace_id is read from ctx (CtxWithTraceID).
func SendCtx(ctx context.Context, topic string, data any, opts ...Option) {
	c := client()
	if c == nil {
		return
	}
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	c.send(topic, data, o.traceID)
}
