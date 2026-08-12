package logtrack

import (
	"sync"
	"testing"
)

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
	// Each subtest resets the once so it sees a fresh env value.
	httpBodySizeLimitOnce = sync.Once{}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("default=%d", got)
	}

	httpBodySizeLimitOnce = sync.Once{}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "2048")
	if got := httpBodySizeLimit(); got != 2048 {
		t.Errorf("env override=%d", got)
	}

	httpBodySizeLimitOnce = sync.Once{}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "garbage")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("invalid env should fall back to default, got %d", got)
	}

	httpBodySizeLimitOnce = sync.Once{}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "-1")
	if got := httpBodySizeLimit(); got != 1024 {
		t.Errorf("negative env should fall back to default, got %d", got)
	}

	httpBodySizeLimitOnce = sync.Once{} // restore
}

func TestTruncateInbound_DropsBodyWhenSizeFieldIsZeroButBodyIsLarge(t *testing.T) {
	// BodySize not set by caller — truncation must measure actual serialized size.
	big := make([]byte, 2048)
	l := &InboundHTTPLog{
		RequestBody:     big, // ~2048 bytes when serialized
		RequestBodySize: 0,   // caller forgot to set size
	}
	truncateInbound(l, 1024)
	if l.RequestBody != nil {
		t.Errorf("large body with zero BodySize must still be dropped")
	}
}

func TestTruncateOutbound_DropsBodyWhenSizeFieldIsZeroButBodyIsLarge(t *testing.T) {
	big := make([]byte, 2048)
	l := &OutboundHTTPLog{
		ResponseBody:     big,
		ResponseBodySize: 0,
	}
	truncateOutbound(l, 1024)
	if l.ResponseBody != nil {
		t.Errorf("large body with zero BodySize must still be dropped")
	}
}

func TestHTTPBodySizeLimit_IsCached(t *testing.T) {
	// httpBodySizeLimit must read the env once and cache the result.
	// Verify that a change to the env after the first call has no effect
	// (i.e. the value was cached, not re-read each time).
	// Reset the cache between test runs by reinitialising the once.
	httpBodySizeLimitOnce = sync.Once{} // reset for this test
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "512")
	first := httpBodySizeLimit()
	if first != 512 {
		t.Fatalf("want 512, got %d", first)
	}
	t.Setenv("LOG_TRACK_HTTP_BODY_SIZE", "9999")
	second := httpBodySizeLimit()
	if second != 512 {
		t.Errorf("cached value changed after env update: got %d, want 512", second)
	}
	httpBodySizeLimitOnce = sync.Once{} // restore for other tests
}

func TestInboundHTTP_NilSafe(t *testing.T) {
	// Must not panic even if no Init has run (client() returns nil) or struct is nil.
	InboundHTTP(nil)
	OutboundHTTP(nil)
	EventTrack(nil)
	RPC(nil)
	App(nil)
}
