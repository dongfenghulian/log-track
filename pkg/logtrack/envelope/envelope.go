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
	TopicAppLogs          = "app-logs"
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
