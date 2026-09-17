package logtrack

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type AppMessage struct {
	EventID      string             `json:"event_id"`
	EventName    string             `json:"event_name"`
	EventTime    int64              `json:"event_time"`
	EventTimeApp int64              `json:"event_time_app,omitempty"`
	Sequence     int64              `json:"sequence,omitempty"`
	RequestID    string             `json:"request_id"`
	SessionID    string             `json:"session_id,omitempty"`
	IsTest       int                `json:"is_test"`
	DeviceUUID   string             `json:"device_uuid"`
	GaidIDFA     string             `json:"gaid_idfa,omitempty"`
	UserID       int64              `json:"user_id"`
	GroupUserID  int64              `json:"group_user_id"`
	IDNumber     string             `json:"id_number,omitempty"`
	Mobile       string             `json:"mobile,omitempty"`
	BID          string             `json:"bid"`
	AppID        int                `json:"app_id"`
	AppVersion   string             `json:"app_version,omitempty"`
	IP           string             `json:"ip,omitempty"`
	FI           map[string]int64   `json:"fi,omitempty"`
	FF           map[string]float64 `json:"ff,omitempty"`
	FS           map[string]string  `json:"fs,omitempty"`
	PayloadJSON  any                `json:"payload_json,omitempty"`
}

func NewAppMessage(eventID, name string) *AppMessage {
	return &AppMessage{EventID: eventID, EventTime: time.Now().UnixMilli(), EventName: name}
}

func messageRequired(fields ...string) error {
	if len(fields)%2 != 0 {
		panic("messageRequired: fields must be key-value pairs")
	}
	for i := 0; i < len(fields); i += 2 {
		if strings.TrimSpace(fields[i+1]) == "" {
			return fmt.Errorf("logtrack: %s is required", fields[i])
		}
	}
	return nil
}

func messageJSONContainer(name string, value any) error {
	if value == nil {
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("logtrack: %s: %w", name, err)
	}
	if len(b) == 0 || (b[0] != '{' && b[0] != '[') {
		return fmt.Errorf("logtrack: %s must be a JSON object or array", name)
	}
	return nil
}

func (m *AppMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("logtrack: nil AppMessage")
	}
	if err := messageRequired("event_id", m.EventID, "event_name", m.EventName, "request_id", m.RequestID, "device_uuid", m.DeviceUUID, "bid", m.BID); err != nil {
		return err
	}
	if m.EventTime <= 0 || m.AppID <= 0 {
		return fmt.Errorf("logtrack: event_time and app_id must be positive")
	}
	if m.IsTest != 0 && m.IsTest != 1 {
		return fmt.Errorf("logtrack: is_test must be 0 or 1")
	}
	return messageJSONContainer("payload_json", m.PayloadJSON)
}

// SendAppMessage sends an app event message to the app.app-event-v1 topic.
func SendAppMessage(m *AppMessage, opts ...Option) error {
	if m == nil {
		return fmt.Errorf("logtrack: nil AppMessage")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	o := applyOpts(opts)
	partitionKey := m.Mobile
	if partitionKey == "" {
		partitionKey = m.DeviceUUID
	}
	if c := client(); c != nil {
		c.send(envelope.TopicAppEvent, m, o.traceID, partitionKey)
	}
	return nil
}

// SendAppMessageCtx is the ctx-aware variant.
func SendAppMessageCtx(ctx context.Context, m *AppMessage, opts ...Option) error {
	o := applyOpts(opts)
	if o.traceID == "" {
		opts = append(opts, WithTraceID(traceIDFromCtx(ctx)))
	}
	return SendAppMessage(m, opts...)
}
