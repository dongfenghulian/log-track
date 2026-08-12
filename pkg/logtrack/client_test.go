package logtrack

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// fakeServer accepts length-prefixed frames and decodes envelopes for assertions.
// It is intentionally not coupled to the real gateway; tests assert exactly what the SDK put on the wire.
type fakeServer struct {
	t       *testing.T
	ln      net.Listener
	mu      sync.Mutex
	got     []envelope.Envelope
	active  int
	failNth atomic.Int32 // close conn after Nth message; 0 disables
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fs := &fakeServer{t: t, ln: ln}
	go fs.acceptLoop()
	return fs
}

func newIdleServer(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				<-done
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close()
	}
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) close() { _ = s.ln.Close() }

func (s *fakeServer) envelopes() []envelope.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]envelope.Envelope, len(s.got))
	copy(out, s.got)
	return out
}

func (s *fakeServer) activeConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *fakeServer) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeServer) handle(conn net.Conn) {
	s.mu.Lock()
	s.active++
	s.mu.Unlock()
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		body := make([]byte, n)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		var env envelope.Envelope
		if err := json.Unmarshal(body, &env); err != nil {
			s.t.Errorf("unmarshal: %v", err)
			return
		}
		s.mu.Lock()
		s.got = append(s.got, env)
		count := len(s.got)
		s.mu.Unlock()

		if fail := int(s.failNth.Load()); fail > 0 && count == fail {
			return // close mid-session to simulate the server hanging up
		}
	}
}

func waitForEnvelopes(t *testing.T, fs *fakeServer, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(fs.envelopes()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("only got %d envelopes, want %d", len(fs.envelopes()), want)
}

func waitForActiveConns(t *testing.T, fs *fakeServer, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fs.activeConns() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active conns=%d, want %d", fs.activeConns(), want)
}

func TestClient_Send_BasicEnvelopeFields(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()

	c, err := New(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "test-svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send("custom-topic", map[string]any{"k": "v"}, "trace-1", "")
	waitForEnvelopes(t, fs, 1, 2*time.Second)

	env := fs.envelopes()[0]
	if env.Version != envelope.Version {
		t.Errorf("version=%q, want %q", env.Version, envelope.Version)
	}
	if env.Topic != "custom-topic" {
		t.Errorf("topic=%q", env.Topic)
	}
	if env.Service != "test-svc" {
		t.Errorf("service=%q", env.Service)
	}
	if env.TraceID != "trace-1" {
		t.Errorf("trace_id=%q", env.TraceID)
	}
	if env.Timestamp == 0 {
		t.Errorf("timestamp not set")
	}
	if env.TimestampAt == "" {
		t.Errorf("timestamp_at not set")
	} else {
		parsed, err := time.Parse(time.RFC3339Nano, env.TimestampAt)
		if err != nil {
			t.Errorf("timestamp_at is not RFC3339: %q", env.TimestampAt)
		} else if _, offset := parsed.Zone(); offset != 8*60*60 {
			t.Errorf("timestamp_at offset=%d, want +08:00", offset)
		}
	}
	host, _ := os.Hostname()
	if env.Host != host {
		t.Errorf("host=%q want %q", env.Host, host)
	}
	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["k"] != "v" {
		t.Errorf("data=%v", data)
	}
}

func TestClient_ShardIndex_DistributesByTraceID(t *testing.T) {
	c, _ := New(&Config{GatewayAddr: "127.0.0.1:1", ServiceName: "x", MaxConns: 4, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer c.Close()

	if got := c.shardIndex(""); got != 0 {
		t.Errorf("empty trace_id should map to shard 0, got %d", got)
	}

	// Same trace_id is deterministic; different IDs should not all collapse to shard 0.
	a := c.shardIndex("trace-A")
	b := c.shardIndex("trace-A")
	if a != b {
		t.Errorf("not deterministic: %d vs %d", a, b)
	}
	seen := map[int]struct{}{}
	for i := 0; i < 100; i++ {
		seen[c.shardIndex("trace-"+string(rune('A'+i%26))+string(rune('0'+i/26)))] = struct{}{}
	}
	if len(seen) < 2 {
		t.Errorf("trace_id hash should hit multiple shards, got %d distinct", len(seen))
	}
}

func TestClient_ReconnectsAfterServerCloses(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()
	fs.failNth.Store(1) // close after first message

	c, err := New(&Config{GatewayAddr: fs.addr(), ServiceName: "svc", MaxConns: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send(envelope.TopicEventTracks, map[string]any{"i": 1}, "", "")
	waitForEnvelopes(t, fs, 1, 2*time.Second)

	// Server closed mid-session. TCP half-close detection takes 1-2 writes:
	// first write often succeeds into the kernel buffer (silently lost), second returns EPIPE
	// which clears the conn, third reconnects and lands. Send several follow-ups; expect at
	// least one to reach the server after reconnect.
	for i := 2; i <= 6; i++ {
		c.send(envelope.TopicEventTracks, map[string]any{"i": i}, "", "")
		time.Sleep(50 * time.Millisecond)
	}
	waitForEnvelopes(t, fs, 2, 3*time.Second) // first + at least one post-reconnect
}

func TestInit_ClosesPreviousDefaultClient(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()
	defer Close()

	if err := Init(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); err != nil {
		t.Fatal(err)
	}
	Send("topic-a", map[string]any{"i": 1})
	waitForEnvelopes(t, fs, 1, 2*time.Second)
	waitForActiveConns(t, fs, 1, 2*time.Second)

	if err := Init(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}); err != nil {
		t.Fatal(err)
	}
	waitForActiveConns(t, fs, 0, 2*time.Second)

	Send("topic-a", map[string]any{"i": 2})
	waitForEnvelopes(t, fs, 2, 2*time.Second)
	waitForActiveConns(t, fs, 1, 2*time.Second)
}

func TestClient_ClosePreventsReconnect(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()

	c, err := New(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	c.send("topic-a", map[string]any{"i": 1}, "", "")
	waitForEnvelopes(t, fs, 1, 2*time.Second)
	waitForActiveConns(t, fs, 1, 2*time.Second)

	c.Close()
	waitForActiveConns(t, fs, 0, 2*time.Second)

	c.send("topic-a", map[string]any{"i": 2}, "", "")
	time.Sleep(100 * time.Millisecond)
	if got := len(fs.envelopes()); got != 1 {
		t.Fatalf("closed client should not send again, got %d envelopes", got)
	}
	if got := fs.activeConns(); got != 0 {
		t.Fatalf("closed client reconnected, active conns=%d", got)
	}
}

func TestClient_DialFailureDoesNotPanic(t *testing.T) {
	// Point at a free port that nobody is listening on.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()

	c, err := New(&Config{GatewayAddr: addr, ServiceName: "svc", ConnectTimeout: 100 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.send("topic", map[string]any{"x": 1}, "trace", "") // must not panic
}

func TestClient_DialFailureBackoffSkipsImmediateReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	c, err := New(&Config{
		GatewayAddr:    addr,
		ServiceName:    "svc",
		MaxConns:       1,
		ConnectTimeout: 50 * time.Millisecond,
		FailureBackoff: 200 * time.Millisecond,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send("topic", map[string]any{"x": 1}, "", "")
	first := c.shards[0].nextAttempt
	if first.IsZero() {
		t.Fatal("dial failure did not set nextAttempt")
	}

	c.send("topic", map[string]any{"x": 2}, "", "")
	if got := c.shards[0].nextAttempt; !got.Equal(first) {
		t.Fatalf("backoff send should not redial or move nextAttempt: got %v want %v", got, first)
	}

	time.Sleep(250 * time.Millisecond)
	c.send("topic", map[string]any{"x": 3}, "", "")
	if got := c.shards[0].nextAttempt; !got.After(first) {
		t.Fatalf("send after backoff should retry and extend nextAttempt: got %v first %v", got, first)
	}
}

func TestClient_EventTracksBypassesFailureBackoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	c, err := New(&Config{
		GatewayAddr:    addr,
		ServiceName:    "svc",
		MaxConns:       1,
		ConnectTimeout: 50 * time.Millisecond,
		FailureBackoff: 5 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send(envelope.TopicEventTracks, map[string]any{"x": 1}, "", "")
	if got := c.shards[0].nextAttempt; !got.IsZero() {
		t.Fatalf("event-tracks should not enter failure backoff, got %v", got)
	}
	c.send(envelope.TopicEventTracks, map[string]any{"x": 2}, "", "")
	if got := c.shards[0].nextAttempt; !got.IsZero() {
		t.Fatalf("event-tracks should keep retrying without backoff, got %v", got)
	}
}

func TestClient_EventTracksReconnectClearsFailureBackoff(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()

	c, err := New(&Config{
		GatewayAddr:    fs.addr(),
		ServiceName:    "svc",
		MaxConns:       1,
		FailureBackoff: 5 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	s := c.shards[0]
	s.mu.Lock()
	s.nextAttempt = time.Now().Add(c.failureBackoff)
	s.mu.Unlock()

	c.send("topic", map[string]any{"i": 1}, "", "")
	if got := len(fs.envelopes()); got != 0 {
		t.Fatalf("normal topic should be skipped during backoff, got %d envelopes", got)
	}

	c.send(envelope.TopicEventTracks, map[string]any{"i": 2}, "", "")
	waitForEnvelopes(t, fs, 1, 2*time.Second)

	s.mu.Lock()
	nextAttempt := s.nextAttempt
	s.mu.Unlock()
	if !nextAttempt.IsZero() {
		t.Fatalf("successful event-tracks reconnect should clear backoff, got %v", nextAttempt)
	}

	c.send("topic", map[string]any{"i": 3}, "", "")
	waitForEnvelopes(t, fs, 2, 2*time.Second)
}

func TestClient_EventTracksWriteFailureBypassesFailureBackoff(t *testing.T) {
	addr, closeFn := newIdleServer(t)
	defer closeFn()

	c, err := New(&Config{
		GatewayAddr:    addr,
		ServiceName:    "svc",
		MaxConns:       1,
		WriteTimeout:   20 * time.Millisecond,
		FailureBackoff: 5 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	large := string(make([]byte, 8*1024*1024))
	for i := 0; i < 20; i++ {
		c.send(envelope.TopicEventTracks, map[string]any{"x": large}, "", "")
		if c.shards[0].conn == nil {
			break
		}
	}
	if got := c.shards[0].nextAttempt; !got.IsZero() {
		t.Fatalf("event-tracks write failure should not enter backoff, got %v", got)
	}
}

func TestClient_WriteFailureBackoffSkipsImmediateReconnect(t *testing.T) {
	addr, closeFn := newIdleServer(t)
	defer closeFn()

	c, err := New(&Config{
		GatewayAddr:    addr,
		ServiceName:    "svc",
		MaxConns:       1,
		WriteTimeout:   20 * time.Millisecond,
		FailureBackoff: 5 * time.Second,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	large := string(make([]byte, 8*1024*1024))
	for i := 0; i < 20; i++ {
		c.send("topic", map[string]any{"x": large}, "", "")
		if !c.shards[0].nextAttempt.IsZero() {
			break
		}
	}

	first := c.shards[0].nextAttempt
	if first.IsZero() {
		t.Fatal("write failure did not set nextAttempt")
	}
	c.send("topic", map[string]any{"x": large}, "", "")
	if got := c.shards[0].nextAttempt; !got.Equal(first) {
		t.Fatalf("backoff send should not redial or move nextAttempt: got %v want %v", got, first)
	}
}

func TestEncodeFrame_RoundTrip(t *testing.T) {
	env := &envelope.Envelope{
		Version:   envelope.Version,
		Topic:     "t",
		Service:   "s",
		Host:      "h",
		Timestamp: 1234,
		Data:      json.RawMessage(`{"x":1}`),
	}
	frame, err := encodeFrame(env)
	if err != nil {
		t.Fatal(err)
	}
	got := binary.BigEndian.Uint32(frame[:4])
	if int(got) != len(frame)-4 {
		t.Errorf("length prefix %d, body %d", got, len(frame)-4)
	}
	var parsed envelope.Envelope
	if err := json.Unmarshal(frame[4:], &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Topic != "t" || parsed.Service != "s" {
		t.Errorf("round-trip mismatch: %+v", parsed)
	}
}

func TestNew_RequiresServiceName(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Errorf("nil cfg should error")
	}
	if _, err := New(&Config{GatewayAddr: "x:1"}); err == nil {
		t.Errorf("missing ServiceName should error")
	}
	// GatewayAddr now defaults to log-track:9583, so omitting it is allowed.
	c, err := New(&Config{ServiceName: "svc"})
	if err != nil {
		t.Errorf("missing GatewayAddr should fall back to default, got error: %v", err)
	}
	if c != nil && c.addr != defaultGatewayAddr {
		t.Errorf("default addr=%q want %q", c.addr, defaultGatewayAddr)
	}
}

func TestNew_AppliesDefaults(t *testing.T) {
	c, err := New(&Config{GatewayAddr: "x:1", ServiceName: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if len(c.shards) != defaultMaxConns {
		t.Errorf("shards=%d", len(c.shards))
	}
	if c.connectTimeout != defaultConnectTimeout {
		t.Errorf("connectTimeout=%v", c.connectTimeout)
	}
	if c.writeTimeout != defaultWriteTimeout {
		t.Errorf("writeTimeout=%v", c.writeTimeout)
	}
	if c.failureBackoff != defaultFailureBackoff {
		t.Errorf("failureBackoff=%v", c.failureBackoff)
	}
}

func TestCtxWithTraceID(t *testing.T) {
	ctx := CtxWithTraceID(context.Background(), "trace-abc")
	if got := traceIDFromCtx(ctx); got != "trace-abc" {
		t.Errorf("got %q", got)
	}
	if got := traceIDFromCtx(nil); got != "" {
		t.Errorf("nil ctx should return empty, got %q", got)
	}
	if got := traceIDFromCtx(context.Background()); got != "" {
		t.Errorf("bare ctx should return empty, got %q", got)
	}
}

func TestApplyOpts_LastWins(t *testing.T) {
	o := applyOpts([]Option{WithTraceID("first"), WithTraceID("second")})
	if o.traceID != "second" {
		t.Errorf("got %q", o.traceID)
	}
}

func TestCtxWithPartitionKey(t *testing.T) {
	ctx := CtxWithPartitionKey(context.Background(), "user-123")
	if got := partitionKeyFromCtx(ctx); got != "user-123" {
		t.Errorf("got %q", got)
	}
	if got := partitionKeyFromCtx(nil); got != "" {
		t.Errorf("nil ctx should return empty, got %q", got)
	}
	if got := partitionKeyFromCtx(context.Background()); got != "" {
		t.Errorf("bare ctx should return empty, got %q", got)
	}
}

func TestCtxKeys_Independent(t *testing.T) {
	// trace_id and partition_key live on separate ctx keys; setting one must not affect the other.
	ctx := context.Background()
	ctx = CtxWithTraceID(ctx, "t-1")
	ctx = CtxWithPartitionKey(ctx, "k-1")
	if got := traceIDFromCtx(ctx); got != "t-1" {
		t.Errorf("trace_id got %q", got)
	}
	if got := partitionKeyFromCtx(ctx); got != "k-1" {
		t.Errorf("partition_key got %q", got)
	}
}

func TestApplyOpts_WithPartitionKey(t *testing.T) {
	o := applyOpts([]Option{WithPartitionKey("k-1"), WithTraceID("t-1")})
	if o.partitionKey != "k-1" {
		t.Errorf("partitionKey=%q", o.partitionKey)
	}
	if o.traceID != "t-1" {
		t.Errorf("traceID=%q", o.traceID)
	}
}

func TestClient_Send_ProducesPartitionKeyOnWire(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()

	c, err := New(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "test-svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send("topic-a", map[string]any{"x": 1}, "trace-1", "user-789")
	waitForEnvelopes(t, fs, 1, 2*time.Second)

	env := fs.envelopes()[0]
	if env.PartitionKey != "user-789" {
		t.Errorf("partition_key=%q want user-789", env.PartitionKey)
	}
	if env.TraceID != "trace-1" {
		t.Errorf("trace_id=%q want trace-1", env.TraceID)
	}
}

func TestClient_Send_OmitsEmptyPartitionKey(t *testing.T) {
	fs := newFakeServer(t)
	defer fs.close()

	c, err := New(&Config{
		GatewayAddr: fs.addr(),
		ServiceName: "test-svc",
		MaxConns:    1,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.send("topic-a", map[string]any{"x": 1}, "trace-1", "")
	waitForEnvelopes(t, fs, 1, 2*time.Second)

	// PartitionKey should not appear in the JSON wire format when empty (omitempty).
	// We assert by re-marshalling and inspecting raw bytes.
	env := fs.envelopes()[0]
	if env.PartitionKey != "" {
		t.Errorf("partition_key should be empty, got %q", env.PartitionKey)
	}
}
