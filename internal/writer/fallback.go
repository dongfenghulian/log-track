package writer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dongfenghulian/log-track/internal/metrics"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// FallbackWriter persists envelopes as JSON Lines into rolling files when Kafka is down.
//
// File naming: <ts>.log (writing) → <ts>.log.done (rolled, ready for replay).
// On Write: append a JSON line; if the file exceeds maxFileSize, rotate.
// On Peek: scan oldest .log.done first, return the first unacked record (record ID = file:offset).
// On Ack: bookkeeping only flushes when the .done file is fully drained → delete it.
//
// This is intentionally simple. We do not maintain an offset cursor on disk: if the gateway crashes
// mid-replay, the next start will replay records that were already sent (at-least-once). Downstream
// dedupe is the consumer's problem, but the at-least-once duplication window is bounded by the
// recovery interval and a single replay batch.
type FallbackWriter struct {
	dir         string
	maxFileSize int64
	maxFiles    int

	mu          sync.Mutex
	current     *os.File
	currentPath string
	currentSize int64

	// peekState tracks the file currently being drained on the recovery side.
	peekState *peekFile
}

type peekFile struct {
	path    string
	scanner *bufio.Scanner
	f       *os.File
}

// FallbackRecord is the unit handed to/acked by Manager.
type FallbackRecord struct {
	Env  *envelope.Envelope
	file string
}

func NewFallbackWriter(dir string, maxFileSize int64, maxFiles int) (*FallbackWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create fallback dir: %w", err)
	}
	fw := &FallbackWriter{
		dir:         dir,
		maxFileSize: maxFileSize,
		maxFiles:    maxFiles,
	}
	if err := fw.adoptStaleLogs(); err != nil {
		return nil, fmt.Errorf("adopt stale logs: %w", err)
	}
	// Reflect any adopted .log.done files in the gauge.
	if dones, err := fw.listDoneFiles(); err == nil {
		metrics.FallbackFilesSet(len(dones))
	}
	return fw, nil
}

// adoptStaleLogs renames any *.log left over from a prior process into *.log.done so they
// become eligible for replay by the recovery loop. Without this, a crash mid-write strands
// the active file: Peek() only reads .log.done, so the data sits forever invisible.
func (f *FallbackWriter) adoptStaleLogs() error {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".log.done") {
			continue
		}
		from := filepath.Join(f.dir, name)
		to := from + ".done"
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("rename %s: %w", name, err)
		}
	}
	return nil
}

// Write appends one envelope as a JSON line. Rotates the active file when it exceeds maxFileSize.
func (f *FallbackWriter) Write(env *envelope.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.current == nil {
		if err := f.openNew(); err != nil {
			return err
		}
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	n, err := f.current.Write(body)
	if err != nil {
		return err
	}
	f.currentSize += int64(n)
	metrics.FallbackWriteInc()

	if f.currentSize >= f.maxFileSize {
		if err := f.rotate(); err != nil {
			return err
		}
	}
	return nil
}

func (f *FallbackWriter) openNew() error {
	name := fmt.Sprintf("%d.log", time.Now().UnixNano())
	p := filepath.Join(f.dir, name)
	file, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open fallback file: %w", err)
	}
	f.current = file
	f.currentPath = p
	f.currentSize = 0
	return nil
}

func (f *FallbackWriter) rotate() error {
	if f.current == nil {
		return nil
	}
	if err := f.current.Close(); err != nil {
		return err
	}
	done := f.currentPath + ".done"
	if err := os.Rename(f.currentPath, done); err != nil {
		return err
	}
	f.current = nil
	f.currentPath = ""
	f.currentSize = 0

	// Enforce maxFiles: oldest .done first.
	dones, _ := f.listDoneFiles()
	if len(dones) > f.maxFiles {
		over := len(dones) - f.maxFiles
		for i := 0; i < over; i++ {
			_ = os.Remove(dones[i])
		}
		dones = dones[over:]
	}
	metrics.FallbackFilesSet(len(dones))
	return nil
}

// listDoneFiles returns all rolled files sorted by name (which sorts by creation time
// because file names are nanosecond timestamps).
func (f *FallbackWriter) listDoneFiles() ([]string, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".log.done") {
			continue
		}
		out = append(out, filepath.Join(f.dir, e.Name()))
	}
	sort.Slice(out, func(i, j int) bool {
		ai := tsFromName(out[i])
		aj := tsFromName(out[j])
		return ai < aj
	})
	return out, nil
}

func tsFromName(path string) int64 {
	name := filepath.Base(path)
	name = strings.TrimSuffix(name, ".log.done")
	n, _ := strconv.ParseInt(name, 10, 64)
	return n
}

// Peek returns the next unacked envelope. On end of one file, advances to the next.
// Caller must call Ack with the returned record after successful replay.
func (f *FallbackWriter) Peek() (*FallbackRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for {
		if f.peekState == nil {
			if err := f.openOldestDone(); err != nil {
				return nil, false
			}
		}
		if f.peekState.scanner.Scan() {
			line := f.peekState.scanner.Bytes()
			var env envelope.Envelope
			if err := json.Unmarshal(line, &env); err != nil {
				continue
			}
			return &FallbackRecord{Env: &env, file: f.peekState.path}, true
		}
		// EOF on this file → close, delete, advance.
		_ = f.peekState.f.Close()
		_ = os.Remove(f.peekState.path)
		f.peekState = nil
	}
}

func (f *FallbackWriter) openOldestDone() error {
	dones, err := f.listDoneFiles()
	if err != nil || len(dones) == 0 {
		return errors.New("no fallback files")
	}
	file, err := os.Open(dones[0])
	if err != nil {
		return err
	}
	f.peekState = &peekFile{
		path:    dones[0],
		f:       file,
		scanner: bufio.NewScanner(io.Reader(file)),
	}
	// Allow lines up to 16MB (a single logged HTTP body could be a few MB even after truncation).
	buf := make([]byte, 0, 1<<16)
	f.peekState.scanner.Buffer(buf, 16*1024*1024)
	return nil
}

// Ack is currently a no-op: Peek returns lines sequentially, and the file is deleted at EOF.
// Kept for symmetry with other writers and to leave room for per-record cursors later.
func (f *FallbackWriter) Ack(*FallbackRecord) {}

// Close flushes the active file (rotates it so it's eligible for replay on next start).
func (f *FallbackWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == nil {
		return nil
	}
	if err := f.current.Sync(); err != nil {
		_ = f.current.Close()
		return err
	}
	if err := f.current.Close(); err != nil {
		return err
	}
	done := f.currentPath + ".done"
	_ = os.Rename(f.currentPath, done)
	f.current = nil
	f.currentPath = ""
	return nil
}
