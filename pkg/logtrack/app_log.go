package logtrack

import (
	"context"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// AppLog is the data payload for topic app-logs (structured application logs).
type AppLog struct {
	AppID    int            `json:"app_id,omitempty"`
	Country  string         `json:"country,omitempty"`
	BID      string         `json:"bid,omitempty"`
	Level    string         `json:"level"`
	Message  string         `json:"message"`
	File     string         `json:"file,omitempty"`
	Line     int            `json:"line,omitempty"`
	Function string         `json:"function,omitempty"`
	Stack    string         `json:"stack,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
}

// App sends a structured application log entry. Level should be one of DEBUG/INFO/WARN/ERROR.
func App(l *AppLog, opts ...Option) {
	if l == nil {
		return
	}
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicAppLogs, l, o.traceID, o.partitionKey)
	}
}

// AppCtx is the ctx-aware variant.
func AppCtx(ctx context.Context, l *AppLog, opts ...Option) {
	if l == nil {
		return
	}
	o := applyOpts(opts)
	if o.traceID == "" {
		o.traceID = traceIDFromCtx(ctx)
	}
	if o.partitionKey == "" {
		o.partitionKey = partitionKeyFromCtx(ctx)
	}
	if c := client(); c != nil {
		c.send(envelope.TopicAppLogs, l, o.traceID, o.partitionKey)
	}
}
