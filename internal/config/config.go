// Package config loads gateway configuration from environment variables.
// All env vars are prefixed with LOG_TRACK_ and have sensible defaults.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server   ServerConfig
	Kafka    KafkaConfig
	Fallback FallbackConfig
	Shutdown ShutdownConfig
}

type ServerConfig struct {
	Address             string
	MaxConnections      int
	QueueSize           int
	WorkerCount         int
	CriticalQueueSize   int
	CriticalWorkerCount int
	MaxMessageSize      int
	MetricsAddress      string // ":9090" by default
}

type KafkaConfig struct {
	Brokers      []string
	BatchSize    int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
}

type FallbackConfig struct {
	DataDir     string
	MaxFileSize int64
	MaxFiles    int
}

type ShutdownConfig struct {
	Timeout           time.Duration
	ConnReadTimeout   time.Duration
	KafkaFlushTimeout time.Duration
}

// Load reads all LOG_TRACK_* env vars, applying defaults where unset.
func Load() Config {
	return Config{
		Server: ServerConfig{
			Address:             getString("LOG_TRACK_SERVER_ADDRESS", ":9583"),
			MaxConnections:      getInt("LOG_TRACK_SERVER_MAX_CONNECTIONS", 3000),
			QueueSize:           getInt("LOG_TRACK_SERVER_QUEUE_SIZE", 5000),
			WorkerCount:         getInt("LOG_TRACK_SERVER_WORKER_COUNT", 30),
			CriticalQueueSize:   getPositiveInt("LOG_TRACK_SERVER_CRITICAL_QUEUE_SIZE", 2000),
			CriticalWorkerCount: getPositiveInt("LOG_TRACK_SERVER_CRITICAL_WORKER_COUNT", 10),
			MaxMessageSize:      getInt("LOG_TRACK_SERVER_MAX_MESSAGE_SIZE", 10*1024*1024),
			MetricsAddress:      getString("LOG_TRACK_METRICS_ADDRESS", ":9584"),
		},
		Kafka: KafkaConfig{
			Brokers:      splitCSV(getString("LOG_TRACK_KAFKA_BROKERS", "kafka:9092")),
			BatchSize:    getInt("LOG_TRACK_KAFKA_BATCH_SIZE", 100),
			BatchTimeout: getDuration("LOG_TRACK_KAFKA_BATCH_TIMEOUT", 10*time.Millisecond),
			WriteTimeout: getDuration("LOG_TRACK_KAFKA_WRITE_TIMEOUT", 2*time.Second),
		},
		Fallback: FallbackConfig{
			DataDir:     getString("LOG_TRACK_FALLBACK_DATA_DIR", "/data/logtrack"),
			MaxFileSize: int64(getInt("LOG_TRACK_FALLBACK_MAX_FILE_SIZE", 100*1024*1024)),
			MaxFiles:    getInt("LOG_TRACK_FALLBACK_MAX_FILES", 10),
		},
		Shutdown: ShutdownConfig{
			Timeout:           getDuration("LOG_TRACK_SHUTDOWN_TIMEOUT", 5*time.Second),
			ConnReadTimeout:   getDuration("LOG_TRACK_SHUTDOWN_CONN_READ_TIMEOUT", 3*time.Second),
			KafkaFlushTimeout: getDuration("LOG_TRACK_SHUTDOWN_KAFKA_FLUSH_TIMEOUT", 3*time.Second),
		},
	}
}

func getString(key, dflt string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return dflt
}

func getInt(key string, dflt int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return dflt
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return dflt
	}
	return n
}

func getPositiveInt(key string, dflt int) int {
	n := getInt(key, dflt)
	if n <= 0 {
		return dflt
	}
	return n
}

func getDuration(key string, dflt time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return dflt
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return dflt
	}
	return d
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
