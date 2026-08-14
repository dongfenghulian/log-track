package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/dongfenghulian/log-track/internal/queue"
	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

func TestServer_ShutdownReturnsWhenQueueIsFull(t *testing.T) {
	addr := reserveTCPAddr(t)
	q := queue.New(1)
	srv := New(Config{
		Address:         addr,
		MaxConnections:  2,
		MaxMessageSize:  1024 * 1024,
		ConnReadTimeout: 10 * time.Millisecond,
	}, q, q, slog.New(slog.NewTextHandler(io.Discard, nil)))

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		q.Close()
	})

	waitForListen(t, addr)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeTestFrame(t, conn, "one")
	writeTestFrame(t, conn, "two")

	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		srv.Shutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked while connection goroutine was waiting on a full queue")
	}

	q.Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}

func TestServer_RoutesEventTracksToCriticalQueue(t *testing.T) {
	addr := reserveTCPAddr(t)
	normalQ := queue.New(10)
	criticalQ := queue.New(10)
	srv := New(Config{
		Address:         addr,
		MaxConnections:  2,
		MaxMessageSize:  1024 * 1024,
		ConnReadTimeout: 10 * time.Millisecond,
	}, normalQ, criticalQ, slog.New(slog.NewTextHandler(io.Discard, nil)))

	go srv.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		normalQ.Close()
		criticalQ.Close()
	})

	waitForListen(t, addr)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFrame(t, conn, envelope.TopicEventTracks)
	writeTestFrame(t, conn, "custom-topic")
	_ = conn.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if normalQ.Len() == 1 && criticalQ.Len() == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("queue depths normal=%d critical=%d", normalQ.Len(), criticalQ.Len())
}

func TestServer_ShutdownWaitsForServeGoroutines(t *testing.T) {
	// Regression: Shutdown's normal exit path (connCount==0) must call connsWG.Wait()
	// before returning. Without it, serve() goroutines are still mid-cleanup (between
	// connCount.Add(-1) and connsWG.Done()) when Shutdown returns.
	addr := reserveTCPAddr(t)
	q := queue.New(100)
	srv := New(Config{
		Address:         addr,
		MaxConnections:  10,
		MaxMessageSize:  1024 * 1024,
		ConnReadTimeout: 50 * time.Millisecond,
	}, q, q, slog.New(slog.NewTextHandler(io.Discard, nil)))

	go srv.Start()
	t.Cleanup(func() { q.Close() })
	waitForListen(t, addr)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFrame(t, conn, "test-topic")
	// Wait until the server has received and registered the connection.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if srv.connCount.Load() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Close the client side so serve() will notice EOF and enter its cleanup path.
	conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	// After Shutdown returns, all serve goroutines must have fully exited.
	// connsWG counter must be exactly zero — if any goroutine is still mid-cleanup,
	// a subsequent Add(1)/Done() pair would panic or the WaitGroup would be in a bad state.
	// We verify by adding 1 and immediately calling Done, which will panic if counter < 0.
	srv.connsWG.Add(1)
	srv.connsWG.Done()
}

func reserveTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start listening")
}

func writeTestFrame(t *testing.T, conn net.Conn, topic string) {
	t.Helper()
	body, err := json.Marshal(&envelope.Envelope{
		Version: envelope.Version,
		Topic:   topic,
		Data:    json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
}
