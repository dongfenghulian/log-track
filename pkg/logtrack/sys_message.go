package logtrack

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

const (
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelFatal = "FATAL"
)

type SysMessage struct {
	EventID      string `json:"event_id"`
	EventTime    int64  `json:"event_time"`
	SourceSystem string `json:"source_system"`
	JobName      string `json:"job_name,omitempty"`
	Component    string `json:"component,omitempty"`
	Env          string `json:"env,omitempty"`
	Level        string `json:"level"`
	EventCode    string `json:"event_code,omitempty"`
	EventType    string `json:"event_type,omitempty"`
	Message      string `json:"message"`
	StackTrace   string `json:"stack_trace,omitempty"`
	Fingerprint  string `json:"fingerprint,omitempty"`
	BID          string `json:"bid,omitempty"`
	AppID        int    `json:"app_id"`
	RequestID    string `json:"request_id,omitempty"`
	EntityRef    string `json:"entity_ref,omitempty"`
	ContextJSON  any    `json:"context_json,omitempty"`
	Host         string `json:"host,omitempty"`
	AppVersion   string `json:"app_version,omitempty"`
}

func NewSysMessage(eventID, level, code, message string) *SysMessage {
	return &SysMessage{EventID: eventID, EventTime: time.Now().UnixMilli(), Level: level, EventCode: code, Message: message}
}

func (m *SysMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("logtrack: nil SysMessage")
	}
	if err := messageRequired("event_id", m.EventID, "source_system", m.SourceSystem, "message", m.Message); err != nil {
		return err
	}
	if m.EventTime <= 0 {
		return fmt.Errorf("logtrack: event_time must be positive")
	}
	if m.Level != LevelWarn && m.Level != LevelError && m.Level != LevelFatal {
		return fmt.Errorf("logtrack: invalid level %q", m.Level)
	}
	if strings.TrimSpace(m.EventCode) == "" && strings.TrimSpace(m.EventType) == "" {
		return fmt.Errorf("logtrack: event_code or event_type is required")
	}
	return messageJSONContainer("context_json", m.ContextJSON)
}

// SendSysMessage sends a system event message to the sys.sys-event-v1 topic.
func SendSysMessage(m *SysMessage, opts ...Option) error {
	if m == nil {
		return fmt.Errorf("logtrack: nil SysMessage")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicSysEvent, m, o.traceID, m.EventID)
	}
	return nil
}

// SendSysMessageCtx is the ctx-aware variant.
func SendSysMessageCtx(ctx context.Context, m *SysMessage, opts ...Option) error {
	o := applyOpts(opts)
	if o.traceID == "" {
		opts = append(opts, WithTraceID(traceIDFromCtx(ctx)))
	}
	return SendSysMessage(m, opts...)
}
