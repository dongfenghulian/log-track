package logtrack

import (
	"context"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// Event is the data payload for topic event-tracks.
type Event struct {
	BID         string         `json:"bid,omitempty"`
	Country     string         `json:"country,omitempty"`
	AppID       int            `json:"app_id,omitempty"`
	Name        string         `json:"event_name"`
	UserID      int64          `json:"user_id,omitempty"`
	SessionUUID string         `json:"session_uuid,omitempty"`
	DeviceUUID  string         `json:"device_uuid,omitempty"`
	Platform    string         `json:"platform"`
	AppVersion  string         `json:"app_version"`
	Properties  map[string]any `json:"properties,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
}

// Event sends a user-behavior event.
func EventTrack(e *Event, opts ...Option) {
	if e == nil {
		return
	}
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicEventTracks, e, o.traceID, o.partitionKey)
	}
}

// EventCtx is the ctx-aware variant.
func EventCtx(ctx context.Context, e *Event, opts ...Option) {
	if e == nil {
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
		c.send(envelope.TopicEventTracks, e, o.traceID, o.partitionKey)
	}
}
