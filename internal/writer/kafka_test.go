package writer

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestKafkaProbe_NoBrokersReturnsError(t *testing.T) {
	kw := NewKafkaWriter(nil, 1, time.Millisecond, 200*time.Millisecond)
	if err := kw.Probe(context.Background()); err == nil {
		t.Errorf("empty broker list should error")
	}
}

func TestKafkaProbe_UnreachableReturnsError(t *testing.T) {
	// Pick a free port then close, so dial fails fast.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	kw := NewKafkaWriter([]string{addr}, 1, time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := kw.Probe(ctx); err == nil {
		t.Errorf("dialing closed port should error")
	}
}

func TestKafkaProbe_DialOKButProtocolFailsReturnsError(t *testing.T) {
	// Stand up a TCP listener that accepts connections but answers nothing.
	// kafka.Conn.Brokers() will fail because the peer doesn't speak Kafka.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold the connection open without sending anything.
			go func(c net.Conn) {
				defer c.Close()
				time.Sleep(2 * time.Second)
			}(conn)
		}
	}()

	kw := NewKafkaWriter([]string{ln.Addr().String()}, 1, time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := kw.Probe(ctx); err == nil {
		t.Errorf("non-kafka peer should fail Brokers()")
	}
}
