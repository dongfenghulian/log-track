package logtrack

import "testing"

func TestTruncateInbound_DropsLargeBodies(t *testing.T) {
	l := &InboundHTTPLog{
		RequestBody:      map[string]string{"k": "v"},
		RequestBodySize:  4096,
		ResponseBody:     "ok",
		ResponseBodySize: 2,
	}
	truncateInbound(l, 1024)
	if l.RequestBody != nil {
		t.Errorf("oversized request body not dropped")
	}
	if l.ResponseBody == nil {
		t.Errorf("small response body should remain")
	}
	if l.RequestBodySize != 4096 {
		t.Errorf("size field must remain")
	}
}

func TestTruncateOutbound_DropsLargeBodies(t *testing.T) {
	l := &OutboundHTTPLog{
		RequestBody:      "ok",
		RequestBodySize:  2,
		ResponseBody:     map[string]string{"big": "data"},
		ResponseBodySize: 99999,
	}
	truncateOutbound(l, 1024)
	if l.RequestBody == nil {
		t.Errorf("small request body should remain")
	}
	if l.ResponseBody != nil {
		t.Errorf("oversized response body not dropped")
	}
}

func TestHTTPBodySizeLimit_EnvOverride(t *testing.T) {
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("default=%d", got)
	}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "2048")
	if got := httpBodySizeLimit(); got != 2048 {
		t.Errorf("env override=%d", got)
	}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "garbage")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("invalid env should fall back to default, got %d", got)
	}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "-1")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("negative env should fall back to default, got %d", got)
	}
}

func TestInboundHTTP_NilSafe(t *testing.T) {
	// Must not panic even if no Init has run (client() returns nil) or struct is nil.
	InboundHTTP(nil)
	OutboundHTTP(nil)
	EventTrack(nil)
	RPC(nil)
	App(nil)
}
