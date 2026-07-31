package server

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/dongfenghulian/log-track/pkg/logtrack/envelope"
)

// readFrame reads one length-prefixed frame from conn and decodes the envelope inside.
// Caller may set a read deadline on conn before calling.
func readFrame(conn net.Conn, maxSize int) (*envelope.Envelope, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, errors.New("zero-length frame")
	}
	if int(n) > maxSize {
		return nil, fmt.Errorf("frame size %d exceeds max %d", n, maxSize)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	var env envelope.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	env.EnsureTimestampAt()
	return &env, nil
}
