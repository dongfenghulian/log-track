package logtrack

import "context"

// Option customizes a single send call. Currently only WithTraceID is provided;
// business fields like app_id/country/bid live in helper structs or the data map.
type Option func(*sendOpts)

type sendOpts struct {
	traceID string
}

func applyOpts(opts []Option) sendOpts {
	var o sendOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithTraceID attaches a trace_id to the envelope for this call.
func WithTraceID(traceID string) Option {
	return func(o *sendOpts) { o.traceID = traceID }
}

// ctx-key for the convenience XxxCtx helpers.
type ctxKey struct{}

// CtxWithTraceID stores a trace_id on the context so downstream XxxCtx helpers can pick it up.
func CtxWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, ctxKey{}, traceID)
}

func traceIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}
