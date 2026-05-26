package logtrack

import (
	"context"
	"os"
	"strconv"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// httpBodySizeLimit returns the configured body truncation threshold (bytes).
// Reads LOG_TRACK_HTTP_BODY_SIZE on every call; cheap and lets ops change it via env without restart-on-change-tooling.
func httpBodySizeLimit() int {
	const dflt = 1024
	v := os.Getenv("LOG_TRACK_HTTP_BODY_SIZE")
	if v == "" {
		return dflt
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return dflt
	}
	return n
}

// InboundHTTPLog is the data payload for topic inbound-http-logs (app -> backend).
type InboundHTTPLog struct {
	AppID            int            `json:"app_id,omitempty"`
	Country          string         `json:"country,omitempty"`
	BID              string         `json:"bid,omitempty"`
	Method           string         `json:"method"`
	URL              string         `json:"url"`
	DurationMs       int64          `json:"duration_ms"`
	ClientIP         string         `json:"client_ip,omitempty"`
	RequestHeaders   map[string]any `json:"request_headers,omitempty"`
	RequestBody      any            `json:"request_body,omitempty"`
	RequestBodyType  string         `json:"request_body_type,omitempty"`
	RequestBodySize  int64          `json:"request_body_size,omitempty"`
	ResponseStatus   int            `json:"response_status"`
	ResponseHeaders  map[string]any `json:"response_headers,omitempty"`
	ResponseBody     any            `json:"response_body,omitempty"`
	ResponseBodyType string         `json:"response_body_type,omitempty"`
	ResponseBodySize int64          `json:"response_body_size,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

// InboundHTTP delivers an inbound HTTP log entry. Bodies larger than LOG_TRACK_HTTP_BODY_SIZE
// are dropped (size fields are preserved).
func InboundHTTP(l *InboundHTTPLog, opts ...Option) {
	if l == nil {
		return
	}
	truncateInbound(l, httpBodySizeLimit())
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicInboundHTTPLogs, l, o.traceID, o.partitionKey)
	}
}

// InboundHTTPCtx is the ctx-aware variant.
func InboundHTTPCtx(ctx context.Context, l *InboundHTTPLog, opts ...Option) {
	if l == nil {
		return
	}
	truncateInbound(l, httpBodySizeLimit())
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	if o.partitionKey == "" {
		o.partitionKey = partitionKeyFromCtx(ctx)
	}
	if c := client(); c != nil {
		c.send(envelope.TopicInboundHTTPLogs, l, o.traceID, o.partitionKey)
	}
}

// OutboundHTTPLog is the data payload for topic outbound-http-logs (backend -> 3rd-party).
type OutboundHTTPLog struct {
	AppID            int            `json:"app_id,omitempty"`
	Country          string         `json:"country,omitempty"`
	BID              string         `json:"bid,omitempty"`
	Provider         string         `json:"provider"`
	Method           string         `json:"method"`
	URL              string         `json:"url"`
	DurationMs       int64          `json:"duration_ms"`
	RequestHeaders   map[string]any `json:"request_headers,omitempty"`
	RequestBody      any            `json:"request_body,omitempty"`
	RequestBodyType  string         `json:"request_body_type,omitempty"`
	RequestBodySize  int64          `json:"request_body_size,omitempty"`
	ResponseStatus   int            `json:"response_status"`
	ResponseHeaders  map[string]any `json:"response_headers,omitempty"`
	ResponseBody     any            `json:"response_body,omitempty"`
	ResponseBodyType string         `json:"response_body_type,omitempty"`
	ResponseBodySize int64          `json:"response_body_size,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

// OutboundHTTP delivers an outbound HTTP log entry.
func OutboundHTTP(l *OutboundHTTPLog, opts ...Option) {
	if l == nil {
		return
	}
	truncateOutbound(l, httpBodySizeLimit())
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicOutboundHTTPLogs, l, o.traceID, o.partitionKey)
	}
}

// OutboundHTTPCtx is the ctx-aware variant.
func OutboundHTTPCtx(ctx context.Context, l *OutboundHTTPLog, opts ...Option) {
	if l == nil {
		return
	}
	truncateOutbound(l, httpBodySizeLimit())
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	if o.partitionKey == "" {
		o.partitionKey = partitionKeyFromCtx(ctx)
	}
	if c := client(); c != nil {
		c.send(envelope.TopicOutboundHTTPLogs, l, o.traceID, o.partitionKey)
	}
}

func truncateInbound(l *InboundHTTPLog, limit int) {
	if l.RequestBody != nil && l.RequestBodySize > int64(limit) {
		l.RequestBody = nil
	}
	if l.ResponseBody != nil && l.ResponseBodySize > int64(limit) {
		l.ResponseBody = nil
	}
}

func truncateOutbound(l *OutboundHTTPLog, limit int) {
	if l.RequestBody != nil && l.RequestBodySize > int64(limit) {
		l.RequestBody = nil
	}
	if l.ResponseBody != nil && l.ResponseBodySize > int64(limit) {
		l.ResponseBody = nil
	}
}
