// Package event handles the event-tracks topic.
package event

import (
	"encoding/json"
	"fmt"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicEventTracks, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicEventTracks }

type payload struct {
	AppID      int    `json:"app_id"`
	BID        string `json:"bid"`
	Country    string `json:"country"`
	EventName  string `json:"event_name"`
	UserID     int64  `json:"user_id"`
	Platform   string `json:"platform"`
	AppVersion string `json:"app_version"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return fmt.Errorf("event-tracks: decode data: %w", err)
	}
	// All errors include the identifying fields so an operator can find the offending caller
	// in slog output. event_name / bid are typically enough to pinpoint a service+code path.
	if p.BID == "" {
		return fmt.Errorf("event-tracks: bid is required (event_name=%q app_id=%d user_id=%d)",
			p.EventName, p.AppID, p.UserID)
	}
	if p.EventName == "" {
		return fmt.Errorf("event-tracks: event_name is required (bid=%q app_id=%d user_id=%d)",
			p.BID, p.AppID, p.UserID)
	}
	if p.Platform == "" || p.AppVersion == "" {
		return fmt.Errorf("event-tracks: platform and app_version are required (event_name=%q bid=%q platform=%q app_version=%q app_id=%d user_id=%d)",
			p.EventName, p.BID, p.Platform, p.AppVersion, p.AppID, p.UserID)
	}
	writeradapter.Write(env)
	return nil
}
