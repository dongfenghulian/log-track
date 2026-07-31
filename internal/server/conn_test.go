package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// fakeConn implements net.Conn over a bytes.Buffer for read-side tests.
type fakeConn struct {
	*bytes.Buffer
}

func (fakeConn) Close() error                     { return nil }
func (fakeConn) LocalAddr() net.Addr              { return nil }
func (fakeConn) RemoteAddr() net.Addr             { return nil }
func (fakeConn) SetDeadline(time.Time) error      { return nil }
func (fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (fakeConn) SetWriteDeadline(time.Time) error { return nil }
func (fakeConn) Write([]byte) (int, error)        { return 0, io.EOF }

func makeFrame(t *testing.T, env *envelope.Envelope) []byte {
	t.Helper()
	body, _ := json.Marshal(env)
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(len(body)))
	copy(out[4:], body)
	return out
}

func TestReadFrame_HappyPath(t *testing.T) {
	env := &envelope.Envelope{Version: envelope.Version, Topic: "t", Service: "s", Timestamp: 1234, Data: []byte(`{}`)}
	conn := fakeConn{Buffer: bytes.NewBuffer(makeFrame(t, env))}
	got, err := readFrame(conn, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "t" || got.Service != "s" {
		t.Errorf("decoded mismatch: %+v", got)
	}
	if got.TimestampAt != "1970-01-01T08:00:01.234+08:00" {
		t.Errorf("timestamp_at=%q", got.TimestampAt)
	}
}

func TestReadFrame_RejectsZeroLength(t *testing.T) {
	conn := fakeConn{Buffer: bytes.NewBuffer([]byte{0, 0, 0, 0})}
	if _, err := readFrame(conn, 1024); err == nil {
		t.Errorf("zero length should error")
	}
}

func TestReadFrame_RejectsOversizeFrame(t *testing.T) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 5000)
	conn := fakeConn{Buffer: bytes.NewBuffer(lenBuf[:])}
	if _, err := readFrame(conn, 1024); err == nil {
		t.Errorf("oversize should error")
	}
}

func TestReadFrame_RejectsTruncatedBody(t *testing.T) {
	frame := makeFrame(t, &envelope.Envelope{Version: envelope.Version, Topic: "x", Data: []byte(`{}`)})
	// Drop the last byte to simulate a truncated body.
	frame = frame[:len(frame)-1]
	conn := fakeConn{Buffer: bytes.NewBuffer(frame)}
	if _, err := readFrame(conn, 1024); err == nil {
		t.Errorf("truncated body should error")
	}
}

func TestReadFrame_RejectsInvalidJSON(t *testing.T) {
	body := []byte("not json at all")
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	conn := fakeConn{Buffer: bytes.NewBuffer(frame)}
	if _, err := readFrame(conn, 1024); err == nil {
		t.Errorf("invalid JSON should error")
	}
}
