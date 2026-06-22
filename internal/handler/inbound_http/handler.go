// Package inbound_http handles the inbound-http-logs topic.
//
// Validation here is intentionally light: the SDK is the source of truth for shape;
// gateway only checks fields that downstream consumers depend on existing.
package inbound_http

import (
	"encoding/json"
	"fmt"

	"github.com/dongfenghulian/log-track/internal/router"
	"github.com/dongfenghulian/log-track/internal/writeradapter"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func init() {
	router.Register(envelope.TopicInboundHTTPLogs, &Handler{})
}

type Handler struct{}

func (h *Handler) Topic() string { return envelope.TopicInboundHTTPLogs }

type payload struct {
	AppID          int    `json:"app_id"`
	BID            string `json:"bid"`
	Country        string `json:"country"`
	Method         string `json:"method"`
	URL            string `json:"url"`
	ResponseStatus int    `json:"response_status"`
}

func (h *Handler) Handle(env *envelope.Envelope) error {
	var p payload
	if err := json.Unmarshal(env.Data, &p); err != nil {
		return fmt.Errorf("inbound-http-logs: decode data: %w", err)
	}
	if p.Method == "" || p.URL == "" {
		return fmt.Errorf("inbound-http-logs: method and url are required (method=%q url=%q app_id=%d bid=%q)",
			p.Method, p.URL, p.AppID, p.BID)
	}
	if p.ResponseStatus == 0 {
		return fmt.Errorf("inbound-http-logs: response_status is required (method=%q url=%q app_id=%d bid=%q)",
			p.Method, p.URL, p.AppID, p.BID)
	}
	writeradapter.Write(env)
	return nil
}
