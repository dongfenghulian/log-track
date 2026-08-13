package logtrack

import (
	"context"
	"encoding/base64"
	"os"
	"reflect"
	"strconv"
	"sync"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

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
	return estimatedJSONSizeExceeds(reflect.ValueOf(body), limit, 0)
}

func estimatedJSONSizeExceeds(v reflect.Value, remaining int, depth int) bool {
	if depth > 32 {
		return true
	}
	return consumeJSONSize(v, &remaining, depth)
}

func consumeJSONSize(v reflect.Value, remaining *int, depth int) bool {
	if !v.IsValid() {
		return consumeSize(remaining, 4)
	}
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return consumeSize(remaining, 4)
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		return consumeSize(remaining, jsonStringLen(v.String()))
	case reflect.Bool:
		if v.Bool() {
			return consumeSize(remaining, 4)
		}
		return consumeSize(remaining, 5)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return consumeSize(remaining, len(strconv.FormatInt(v.Int(), 10)))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return consumeSize(remaining, len(strconv.FormatUint(v.Uint(), 10)))
	case reflect.Float32:
		return consumeSize(remaining, len(strconv.FormatFloat(v.Float(), 'g', -1, 32)))
	case reflect.Float64:
		return consumeSize(remaining, len(strconv.FormatFloat(v.Float(), 'g', -1, 64)))
	case reflect.Slice:
		if v.IsNil() {
			return consumeSize(remaining, 4)
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return consumeSize(remaining, base64.StdEncoding.EncodedLen(v.Len())+2)
		}
		return consumeArraySize(v, remaining, depth)
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return consumeSize(remaining, base64.StdEncoding.EncodedLen(v.Len())+2)
		}
		return consumeArraySize(v, remaining, depth)
	case reflect.Map:
		if v.IsNil() {
			return consumeSize(remaining, 4)
		}
		if consumeSize(remaining, 1) {
			return true
		}
		iter := v.MapRange()
		first := true
		for iter.Next() {
			if !first && consumeSize(remaining, 1) {
				return true
			}
			first = false
			if consumeJSONMapKey(iter.Key(), remaining) || consumeSize(remaining, 1) || consumeJSONSize(iter.Value(), remaining, depth+1) {
				return true
			}
		}
		return consumeSize(remaining, 1)
	case reflect.Struct:
		if consumeSize(remaining, 1) {
			return true
		}
		first := true
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if t.Field(i).PkgPath != "" {
				continue
			}
			if !first && consumeSize(remaining, 1) {
				return true
			}
			first = false
			if consumeSize(remaining, jsonStringLen(t.Field(i).Name)) || consumeSize(remaining, 1) || consumeJSONSize(v.Field(i), remaining, depth+1) {
				return true
			}
		}
		return consumeSize(remaining, 1)
	default:
		return false
	}
}

func consumeArraySize(v reflect.Value, remaining *int, depth int) bool {
	if consumeSize(remaining, 1) {
		return true
	}
	for i := 0; i < v.Len(); i++ {
		if i > 0 && consumeSize(remaining, 1) {
			return true
		}
		if consumeJSONSize(v.Index(i), remaining, depth+1) {
			return true
		}
	}
	return consumeSize(remaining, 1)
}

func consumeJSONMapKey(v reflect.Value, remaining *int) bool {
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return consumeSize(remaining, 6)
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return consumeSize(remaining, jsonStringLen(v.String()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return consumeSize(remaining, len(strconv.FormatInt(v.Int(), 10))+2)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return consumeSize(remaining, len(strconv.FormatUint(v.Uint(), 10))+2)
	default:
		return consumeSize(remaining, 2)
	}
}

func consumeSize(remaining *int, n int) bool {
	*remaining -= n
	return *remaining < 0
}

func jsonStringLen(s string) int {
	n := 2
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' || c == '"':
			n += 2
		case c < 0x20:
			n += 6
		default:
			n++
		}
	}
	return n
}
