package logtrack

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

var errBodyTooLarge = errors.New("logtrack: body exceeds size limit")

var (
	httpBodySizeLimitOnce   sync.Once
	httpBodySizeLimitCached int
)

// httpBodySizeLimit returns the configured body truncation threshold (bytes).
// Read once from LOG_TRACK_HTTP_BODY_SIZE at first call and cached thereafter.
func httpBodySizeLimit() int {
	httpBodySizeLimitOnce.Do(func() {
		const dflt = 1024
		v := os.Getenv("LOG_TRACK_HTTP_BODY_SIZE")
		if v == "" {
			httpBodySizeLimitCached = dflt
			return
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			httpBodySizeLimitCached = dflt
			return
		}
		httpBodySizeLimitCached = n
	})
	return httpBodySizeLimitCached
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
	if l.RequestBody != nil && bodyExceedsLimit(l.RequestBody, l.RequestBodySize, limit) {
		l.RequestBody = nil
	}
	if l.ResponseBody != nil && bodyExceedsLimit(l.ResponseBody, l.ResponseBodySize, limit) {
		l.ResponseBody = nil
	}
}

func truncateOutbound(l *OutboundHTTPLog, limit int) {
	if l.RequestBody != nil && bodyExceedsLimit(l.RequestBody, l.RequestBodySize, limit) {
		l.RequestBody = nil
	}
	if l.ResponseBody != nil && bodyExceedsLimit(l.ResponseBody, l.ResponseBodySize, limit) {
		l.ResponseBody = nil
	}
}

// bodyExceedsLimit returns true when the body is larger than limit bytes.
// It trusts the caller-supplied size when available; otherwise it measures by serializing.
func bodyExceedsLimit(body any, declaredSize int64, limit int) bool {
	if declaredSize > int64(limit) {
		return true
	}
	if declaredSize > 0 {
		return false
	}
	err := json.NewEncoder(&limitWriter{remaining: limit}).Encode(body)
	return errors.Is(err, errBodyTooLarge)
}

type limitWriter struct {
	remaining int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errBodyTooLarge
	}
	w.remaining -= len(p)
	return len(p), nil
}
