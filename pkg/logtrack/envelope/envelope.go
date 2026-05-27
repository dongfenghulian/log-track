// Package envelope defines the wire-format envelope shared by the SDK and the gateway.
package envelope

import "encoding/json"

const Version = "1.0"

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
	TraceID      string          `json:"trace_id,omitempty"`
	PartitionKey string          `json:"partition_key,omitempty"`
	Data         json.RawMessage `json:"data"`
}
