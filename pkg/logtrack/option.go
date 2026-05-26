package logtrack

import "context"

// Option customizes a single send call. Currently provides WithTraceID and WithPartitionKey;
// business fields like app_id/country/bid live in helper structs or the data map.
type Option func(*sendOpts)

type sendOpts struct {
	traceID      string
	partitionKey string
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

// WithPartitionKey overrides the Kafka partition key.
// If unset, the gateway falls back to trace_id; if both are empty, messages distribute round-robin.
func WithPartitionKey(key string) Option {
	return func(o *sendOpts) { o.partitionKey = key }
}

// ctx-keys for the convenience XxxCtx helpers.
type traceIDCtxKey struct{}
type partitionKeyCtxKey struct{}

// CtxWithTraceID stores a trace_id on the context so downstream XxxCtx helpers can pick it up.
func CtxWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDCtxKey{}, traceID)
}

// CtxWithPartitionKey stores a partition key on the context.
func CtxWithPartitionKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, partitionKeyCtxKey{}, key)
}

func traceIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(traceIDCtxKey{}).(string)
	return v
}

func partitionKeyFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(partitionKeyCtxKey{}).(string)
	return v
}
