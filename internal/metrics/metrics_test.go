package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_ServesPrometheusFormat(t *testing.T) {
	// Trigger every metric at least once.
	ConnInc()
	ConnDec()
	MessageObserved("inbound-http-logs", "handled")
	MessageObserved("inbound-http-logs", "invalid")
	QueueDepthSet(7)
	QueueDepthSetForQueue("normal", 3)
	QueueDepthSetForQueue("critical", 2)
	QueueDropInc()
	KafkaWrite("inbound-http-logs", "success", 0.005)
	KafkaWrite("inbound-http-logs", "error", 0.5)
	KafkaHealthy(false)
	KafkaHealthy(true)
	FallbackFilesSet(3)
	FallbackWriteInc()
	FallbackReplay("success")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("status=%d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"logtrack_gateway_connections",
		"logtrack_gateway_messages_total",
		"logtrack_gateway_queue_depth",
		`logtrack_gateway_queue_depth{queue="normal"} 3`,
		`logtrack_gateway_queue_depth{queue="critical"} 2`,
		"logtrack_kafka_writes_total",
		"logtrack_kafka_write_latency_seconds",
		"logtrack_kafka_healthy",
		"logtrack_fallback_files",
		"logtrack_fallback_writes_total",
		"logtrack_fallback_replays_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing metric %q in response", want)
		}
	}
}
