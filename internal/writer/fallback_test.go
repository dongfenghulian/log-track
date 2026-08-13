package writer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestFallback_PeekDoesNotDeleteFileOnScannerError(t *testing.T) {
	// If scanner.Scan() returns false due to an I/O/buffer error (not clean EOF),
	// Peek must NOT delete the file — that would silently destroy unread records.
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 64*1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Write a valid record first.
	if err := fw.Write(&envelope.Envelope{
		Version: envelope.Version, Topic: "first", Data: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	// Write a record whose JSON line is larger than the scanner's 16MB buffer limit.
	// bufio.Scanner will return Scan()=false with scanner.Err()=bufio.ErrTooLong.
	huge := strings.Repeat("x", 17*1024*1024) // 17MB > 16MB scanner buffer
	if err := fw.Write(&envelope.Envelope{
		Version: envelope.Version, Topic: "huge",
		Data: json.RawMessage(`{"x":"` + huge + `"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	dones, _ := filepath.Glob(filepath.Join(dir, "*.log.done"))
	if len(dones) != 1 {
		t.Fatalf("expected 1 .done file, got %v", dones)
	}

	fw2, err := NewFallbackWriter(dir, 64*1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw2.Close()

	// Should return the first valid record.
	rec, ok := fw2.Peek()
	if !ok || rec.Env.Topic != "first" {
		t.Errorf("expected first record, got ok=%v rec=%v", ok, rec)
	}
	// Next Peek hits the oversized line: scanner error → must return (nil, false) without deleting file.
	rec2, ok2 := fw2.Peek()
	if ok2 || rec2 != nil {
		t.Errorf("expected Peek to stop on scanner error, got ok=%v", ok2)
	}

	// The .done file must still exist — it was not cleanly EOF'd.
	if _, err := os.Stat(dones[0]); os.IsNotExist(err) {
		t.Errorf("Peek deleted the fallback file on scanner error — unread records are lost")
	}
}

func TestFallback_AdoptsStaleLogOnStartup(t *testing.T) {
	dir := t.TempDir()

	// Simulate a previous process: write some envelopes, then "crash" without closing
	// (no rotate, leave the .log behind).
	fw1, _ := NewFallbackWriter(dir, 1024*1024, 10)
	for i := 0; i < 3; i++ {
		if err := fw1.Write(&envelope.Envelope{
			Version: envelope.Version, Topic: "t",
			Timestamp: int64(i), Data: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Crash: don't call Close(), just abandon the writer. Active file remains as *.log.
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	dones, _ := filepath.Glob(filepath.Join(dir, "*.log.done"))
	if len(logs) != 1 || len(dones) != 0 {
		t.Fatalf("pre-crash state: logs=%v dones=%v", logs, dones)
	}

	// New process boots: NewFallbackWriter should rename the orphan .log to .log.done.
	fw2, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw2.Close()

	logs, _ = filepath.Glob(filepath.Join(dir, "*.log"))
	dones, _ = filepath.Glob(filepath.Join(dir, "*.log.done"))
	if len(logs) != 0 || len(dones) != 1 {
		t.Fatalf("post-adopt state: logs=%v dones=%v", logs, dones)
	}

	// And the records inside should be replayable.
	var seen []int64
	for {
		rec, ok := fw2.Peek()
		if !ok {
			break
		}
		seen = append(seen, rec.Env.Timestamp)
	}
	if len(seen) != 3 {
		t.Errorf("after adopt, expected 3 records, got %v", seen)
	}
}

func TestFallback_WriteRecoverAfterWriteError(t *testing.T) {
	// When f.current.Write fails, the fd must be closed and niled so the next
	// Write call opens a fresh file instead of retrying the same broken fd.
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()

	// Inject a read-only file as the active write target to force a write error.
	badPath := filepath.Join(dir, "bad.log")
	if err := os.WriteFile(badPath, nil, 0o444); err != nil {
		t.Fatal(err)
	}
	badFile, err := os.OpenFile(badPath, os.O_RDONLY, 0o444)
	if err != nil {
		t.Fatal(err)
	}
	fw.mu.Lock()
	fw.current = badFile
	fw.currentPath = badPath
	fw.currentSize = 0
	fw.mu.Unlock()

	env := &envelope.Envelope{Version: envelope.Version, Topic: "t", Data: json.RawMessage(`{}`)}

	// First write must fail.
	if err := fw.Write(env); err == nil {
		t.Fatal("expected write to fail on read-only fd, got nil")
	}

	// After the failure, f.current must be nil — broken fd discarded.
	fw.mu.Lock()
	stillSet := fw.current != nil
	fw.mu.Unlock()
	if stillSet {
		t.Error("f.current was not cleared after write error")
	}

	// Second write must succeed — opens a new file.
	if err := fw.Write(env); err != nil {
		t.Fatalf("write after error recovery failed: %v", err)
	}
}

func TestFallback_PeekUpdatesFallbackFilesGauge(t *testing.T) {
	// Peek() deletes a .log.done file when it reaches EOF, but the fallback_files gauge
	// must be decremented at that point — not only after rotate().
	dir := t.TempDir()
	fw, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := fw.Write(&envelope.Envelope{Version: envelope.Version, Topic: "t", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := fw.Close(); err != nil { // rotates to .log.done, gauge set to 1
		t.Fatal(err)
	}

	fw2, err := NewFallbackWriter(dir, 1024*1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer fw2.Close()

	if got := testutil.ToFloat64(metrics.FallbackFilesGauge()); got != 1 {
		t.Fatalf("pre-peek gauge = %v, want 1", got)
	}

	// Drain the file completely — EOF causes deletion.
	for {
		rec, ok := fw2.Peek()
		if !ok {
			break
		}
		fw2.Ack(rec)
	}

	// After the file is drained and deleted, the gauge must reflect 0.
	if got := testutil.ToFloat64(metrics.FallbackFilesGauge()); got != 0 {
		t.Errorf("post-peek gauge = %v, want 0 (Peek did not update fallback_files gauge on file deletion)", got)
	}
}
