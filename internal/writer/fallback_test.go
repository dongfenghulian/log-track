package writer

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestFallback_WriteAndPeek(t *testing.T) {
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	for i := 0; i < 3; i++ {
		env := &envelope.Envelope{
			Version:   envelope.Version,
			Topic:     "t",
			Service:   "s",
			Host:      "h",
			Timestamp: int64(i),
			Data:      json.RawMessage(`{}`),
		}
		if err := fw.Write(env); err != nil {
			t.Fatal(err)
		}
	}
	// Close rotates the active .log to .log.done so Peek can read it.
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	fw2, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw2.Close()

	var seen []int64
	for {
		rec, ok := fw2.Peek()
		if !ok {
			break
		}
		seen = append(seen, rec.Env.Timestamp)
		fw2.Ack(rec)
	}
	if len(seen) != 3 {
		t.Fatalf("seen=%v", seen)
	}
	if seen[0] != 0 || seen[1] != 1 || seen[2] != 2 {
		t.Errorf("order broken: %v", seen)
	}
}

func TestFallback_RotationOnSize(t *testing.T) {
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 200, 10) // 200 bytes triggers rotation fast
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	for i := 0; i < 20; i++ {
		env := &envelope.Envelope{
			Version: envelope.Version,
			Topic:   "t",
			Service: "service-with-long-enough-name-to-fill-bytes",
			Data:    json.RawMessage(`{"k":"v"}`),
		}
		if err := fw.Write(env); err != nil {
			t.Fatal(err)
		}
	}

	dones, _ := filepath.Glob(filepath.Join(dir, "*.log.done"))
	if len(dones) < 2 {
		t.Errorf("expected multiple rolled files, got %d", len(dones))
	}
}

func TestFallback_MaxFilesEnforced(t *testing.T) {
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 100, 3) // tiny size + cap of 3 done files
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	for i := 0; i < 50; i++ {
		env := &envelope.Envelope{
			Version: envelope.Version,
			Topic:   "t",
			Service: "service-with-long-enough-name-to-fill-bytes",
			Data:    json.RawMessage(`{}`),
		}
		if err := fw.Write(env); err != nil {
			t.Fatal(err)
		}
	}
	dones, _ := filepath.Glob(filepath.Join(dir, "*.log.done"))
	if len(dones) > 3 {
		t.Errorf("max_files breached: %d > 3 (%v)", len(dones), dones)
	}
}

func TestFallback_PeekStopsWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	if rec, ok := fw.Peek(); ok {
		t.Errorf("expected empty, got %v", rec)
	}
}

func TestFallback_CloseRotatesActiveFile(t *testing.T) {
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := fw.Write(&envelope.Envelope{Version: envelope.Version, Topic: "t", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}
	dones, _ := filepath.Glob(filepath.Join(dir, "*.log.done"))
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(dones) != 1 || len(logs) != 0 {
		t.Errorf("after close: dones=%v logs=%v", dones, logs)
	}
}

func TestTsFromName(t *testing.T) {
	got := tsFromName("/some/path/123456789.log.done")
	if got != 123456789 {
		t.Errorf("got %d", got)
	}
	bad := tsFromName("/some/path/garbage.log.done")
	if bad != 0 {
		t.Errorf("non-numeric should yield 0, got %d", bad)
	}
}

func TestFallback_HandlesGarbageLines(t *testing.T) {
	dir := t.TempDir()
	fw, _ := NewFallbackWriter(dir, 1024*1024, 10)
	if err := fw.Write(&envelope.Envelope{Version: envelope.Version, Topic: "t", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// Inject a malformed line directly into the active file before close.
	fw.mu.Lock()
	_, _ = fw.current.Write([]byte("not valid json\n"))
	fw.mu.Unlock()
	if err := fw.Write(&envelope.Envelope{Version: envelope.Version, Topic: "t2", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	fw2, _ := NewFallbackWriter(dir, 1024*1024, 10)
	defer fw2.Close()

	topics := []string{}
	for {
		rec, ok := fw2.Peek()
		if !ok {
			break
		}
		topics = append(topics, rec.Env.Topic)
	}
	joined := strings.Join(topics, ",")
	if joined != "t,t2" {
		t.Errorf("expected garbage line skipped, got topics=%q", joined)
	}
}
