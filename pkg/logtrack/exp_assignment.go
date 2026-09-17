package logtrack

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

type ExpAssignmentMessage struct {
	ExperimentID   string `json:"experiment_id"`
	SubjectID      string `json:"subject_id"`
	Variant        string `json:"variant"`
	IsActive       *int   `json:"is_active,omitempty"`
	AssignedTimeMS int64  `json:"assigned_time_ms"`
	EventID        string `json:"event_id,omitempty"`
}

func NewExpAssignmentMessage(eventID, experimentID, subjectID, variant string, assignedTimeMS int64) *ExpAssignmentMessage {
	return &ExpAssignmentMessage{
		EventID:        eventID,
		ExperimentID:   experimentID,
		SubjectID:      subjectID,
		Variant:        variant,
		AssignedTimeMS: assignedTimeMS,
	}
}

func (m *ExpAssignmentMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("logtrack: nil ExpAssignmentMessage")
	}
	if err := messageRequired("experiment_id", m.ExperimentID, "subject_id", m.SubjectID, "variant", m.Variant); err != nil {
		return err
	}
	if m.AssignedTimeMS <= 0 {
		return fmt.Errorf("logtrack: assigned_time_ms must be positive")
	}
	if m.IsActive != nil && *m.IsActive != 0 && *m.IsActive != 1 {
		return fmt.Errorf("logtrack: is_active must be 0 or 1")
	}
	for _, field := range []struct{ name, value string }{
		{"experiment_id", m.ExperimentID},
		{"subject_id", m.SubjectID},
		{"variant", m.Variant},
		{"event_id", m.EventID},
	} {
		if !utf8.ValidString(field.value) {
			return fmt.Errorf("logtrack: %s must be valid UTF-8", field.name)
		}
		for _, r := range field.value {
			if unicode.IsControl(r) {
				return fmt.Errorf("logtrack: %s must not contain control characters", field.name)
			}
		}
	}
	return nil
}

// SendExpAssignmentMessage sends an experiment assignment message to the dw.exp-assignment-v1 topic.
// The Kafka partition key is a JSON pair [experiment_id, subject_id].
func SendExpAssignmentMessage(m *ExpAssignmentMessage, opts ...Option) error {
	if m == nil {
		return fmt.Errorf("logtrack: nil ExpAssignmentMessage")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	key, err := json.Marshal([2]string{m.ExperimentID, m.SubjectID})
	if err != nil {
		return fmt.Errorf("logtrack: encode assignment key: %w", err)
	}
	o := applyOpts(opts)
	if c := client(); c != nil {
		c.send(envelope.TopicExpAssignment, m, o.traceID, string(key))
	}
	return nil
}

// SendExpAssignmentMessageCtx is the ctx-aware variant.
func SendExpAssignmentMessageCtx(ctx context.Context, m *ExpAssignmentMessage, opts ...Option) error {
	o := applyOpts(opts)
	if o.traceID == "" {
		opts = append(opts, WithTraceID(traceIDFromCtx(ctx)))
	}
	return SendExpAssignmentMessage(m, opts...)
}
