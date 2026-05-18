// Package event handles the event-tracks topic.
package event

import (
	"encoding/json"
	"errors"

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
	BID        string `json:"bid"`
	EventName  string `json:"event_name"`
	Platform   string `json:"platform"`
	AppVersion string `json:"app_version"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return err
	}
	if p.BID == "" {
		return errors.New("event-tracks: bid is required")
	}
	if p.EventName == "" {
		return errors.New("event-tracks: event_name is required")
	}
	if p.Platform == "" || p.AppVersion == "" {
		return errors.New("event-tracks: platform and app_version are required")
	}
	writeradapter.Write(env)
	return nil
}
