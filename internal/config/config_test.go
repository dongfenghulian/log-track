package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any caller-set env vars so defaults apply.
	for _, k := range []string{
		"LOG_TRACK_SERVER_ADDRESS", "LOG_TRACK_SERVER_QUEUE_SIZE",
		"LOG_TRACK_SERVER_CRITICAL_QUEUE_SIZE", "LOG_TRACK_SERVER_CRITICAL_WORKER_COUNT",
		"LOG_TRACK_KAFKA_BROKERS", "LOG_TRACK_KAFKA_BATCH_TIMEOUT",
		"LOG_TRACK_FALLBACK_DATA_DIR", "LOG_TRACK_SHUTDOWN_TIMEOUT",
		"LOG_TRACK_METRICS_ADDRESS",
	} {
		t.Setenv(k, "")
	}
	c := Load()
	if c.Server.Address != ":9583" {
		t.Errorf("address=%q", c.Server.Address)
	}
	if c.Server.QueueSize != 5000 {
		t.Errorf("queue_size=%d", c.Server.QueueSize)
	}
	if c.Server.MaxConnections != 3000 {
		t.Errorf("max_connections=%d", c.Server.MaxConnections)
	}
	if c.Server.WorkerCount != 30 {
		t.Errorf("worker_count=%d", c.Server.WorkerCount)
	}
	if c.Server.CriticalQueueSize != 2000 {
		t.Errorf("critical_queue_size=%d", c.Server.CriticalQueueSize)
	}
	if c.Server.CriticalWorkerCount != 10 {
		t.Errorf("critical_worker_count=%d", c.Server.CriticalWorkerCount)
	}
	if len(c.Kafka.Brokers) != 1 || c.Kafka.Brokers[0] != "kafka:9092" {
		t.Errorf("brokers=%v", c.Kafka.Brokers)
	}
	if c.Kafka.BatchTimeout != 10*time.Millisecond {
		t.Errorf("batch_timeout=%v", c.Kafka.BatchTimeout)
	}
	if c.Kafka.WriteTimeout != 2*time.Second {
		t.Errorf("kafka_write_timeout=%v", c.Kafka.WriteTimeout)
	}
	if c.Fallback.DataDir != "/data/logtrack" {
		t.Errorf("data_dir=%q", c.Fallback.DataDir)
	}
	if c.Shutdown.Timeout != 5*time.Second {
		t.Errorf("shutdown_timeout=%v", c.Shutdown.Timeout)
	}
	if c.Server.MetricsAddress != ":9584" {
		t.Errorf("metrics_address=%q", c.Server.MetricsAddress)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("LOG_TRACK_SERVER_ADDRESS", "0.0.0.0:9999")
	t.Setenv("LOG_TRACK_KAFKA_BROKERS", "a:1,b:2 , c:3")
	t.Setenv("LOG_TRACK_KAFKA_BATCH_TIMEOUT", "250ms")
	t.Setenv("LOG_TRACK_SERVER_QUEUE_SIZE", "777")
	t.Setenv("LOG_TRACK_SERVER_CRITICAL_QUEUE_SIZE", "333")
	t.Setenv("LOG_TRACK_SERVER_CRITICAL_WORKER_COUNT", "9")
	c := Load()
	if c.Server.Address != "0.0.0.0:9999" {
		t.Errorf("address=%q", c.Server.Address)
	}
	if c.Server.QueueSize != 777 {
		t.Errorf("queue_size=%d", c.Server.QueueSize)
	}
	if c.Server.CriticalQueueSize != 333 {
		t.Errorf("critical_queue_size=%d", c.Server.CriticalQueueSize)
	}
	if c.Server.CriticalWorkerCount != 9 {
		t.Errorf("critical_worker_count=%d", c.Server.CriticalWorkerCount)
	}
	if got := c.Kafka.Brokers; len(got) != 3 || got[0] != "a:1" || got[1] != "b:2" || got[2] != "c:3" {
		t.Errorf("brokers=%v", got)
	}
	if c.Kafka.BatchTimeout != 250*time.Millisecond {
		t.Errorf("batch_timeout=%v", c.Kafka.BatchTimeout)
	}
}

func TestLoad_InvalidEnvFallsBackToDefault(t *testing.T) {
	t.Setenv("LOG_TRACK_SERVER_QUEUE_SIZE", "not-a-number")
	t.Setenv("LOG_TRACK_KAFKA_BATCH_TIMEOUT", "not-a-duration")
	c := Load()
	if c.Server.QueueSize != 5000 {
		t.Errorf("invalid int should fall back, got %d", c.Server.QueueSize)
	}
	if c.Kafka.BatchTimeout != 10*time.Millisecond {
		t.Errorf("invalid duration should fall back, got %v", c.Kafka.BatchTimeout)
	}
}

func TestSplitCSV_TrimsWhitespaceAndEmpties(t *testing.T) {
	got := splitCSV(" a , b ,, c,")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d (%v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d]=%q want %q", i, got[i], want[i])
		}
	}
}
