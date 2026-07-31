// Package envelope defines the wire-format envelope shared by the SDK and the gateway.
package envelope

import (
	"encoding/json"
	"time"
)

const Version = "1.0"

var timestampAtLocation = time.FixedZone("UTC+8", 8*60*60)

// Built-in topic names. Custom topics are allowed; they are written to Kafka as-is via the passthrough handler.
const (
	TopicInboundHTTPLogs  = "inbound-http-logs"
	TopicOutboundHTTPLogs = "outbound-http-logs"
	TopicEventTracks      = "event-tracks"
	TopicRPCCalls         = "rpc-calls"

	// TopicAppLogs is the SDK-side topic name; the gateway accepts envelopes with this topic
	// and rewrites env.Topic to one of the per-level topics below before writing to Kafka.
	TopicAppLogs = "app-logs"

	// Per-level Kafka topics for application logs. The gateway writes to these; the SDK does not.
	TopicAppLogsError = "app-logs-error"
	TopicAppLogsWarn  = "app-logs-warn"
	TopicAppLogsInfo  = "app-logs-info"
	TopicAppLogsDebug = "app-logs-debug"
)

// Envelope is the unified message wrapper. See PROTOCOL.md §1.2.
type Envelope struct {
	Version      string          `json:"version"`
	Topic        string          `json:"topic"`
	Service      string          `json:"service"`
	Host         string          `json:"host"`
	Timestamp    int64           `json:"timestamp"`
	TimestampAt  string          `json:"timestamp_at,omitempty"`
	TraceID      string          `json:"trace_id,omitempty"`
	PartitionKey string          `json:"partition_key,omitempty"`
	Data         json.RawMessage `json:"data"`
}

// TimestampAtFromTime formats an event time for the timestamp_at envelope field.
func TimestampAtFromTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(timestampAtLocation).Format(time.RFC3339Nano)
}

// TimestampAtFromMillis formats a millisecond Unix timestamp for timestamp_at.
func TimestampAtFromMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return TimestampAtFromTime(time.UnixMilli(ms))
}

// EnsureTimestampAt backfills timestamp_at for envelopes produced by older SDKs.
func (e *Envelope) EnsureTimestampAt() {
	if e.TimestampAt == "" {
		e.TimestampAt = TimestampAtFromMillis(e.Timestamp)
	}
}
