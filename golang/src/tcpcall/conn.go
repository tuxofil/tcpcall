/**
Message-oriented socket.

Provides a TCP connection for transferring messages.
Each message has its 32bit length.

Sending of messages is synchronous (see Send() method).
Reading of incoming messages is implemented as background
thread. 'handler' callback is called for each incoming
message.
*/

package tcpcall

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Errors
var (
	MsgConnNotConnectedError = errors.New("msg conn: not connected")
	MsgTooLongError          = errors.New("incoming message is too long")
)

// Message oriented connection
type MsgConn struct {
	socket    net.Conn
	buffer    *bufio.Writer
	lastFlush time.Time
	// socket write mutex
	socketMu sync.Mutex
	// maximum allowed length for incoming packets
	MaxPacketLen int
	// Minimum time between write buffer flushes
	minFlushPeriod time.Duration
	// incoming package handler
	handler func([]byte)
	// callback for disconnect event
	onDisconnect func()
	closed       bool
}

// Create new message oriented connection.
func NewMsgConn(socket net.Conn, minFlushPeriod time.Duration,
	writeBufferSize int,
	handler func([]byte), onClose func()) (*MsgConn, error) {
	if err := socket.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	conn := &MsgConn{
		socket:         socket,
		buffer:         bufio.NewWriterSize(socket, writeBufferSize),
		socketMu:       sync.Mutex{},
		handler:        handler,
		onDisconnect:   onClose,
		minFlushPeriod: minFlushPeriod,
	}
	go conn.readLoop()
	return conn, nil
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// A zero value for t means Write will not time out.
func (c *MsgConn) SetWriteDeadline(t time.Time) error {
	return c.socket.SetWriteDeadline(t)
}

// Send message to the other side.
func (c *MsgConn) Send(msg [][]byte) error {
	if c == nil {
		return MsgConnNotConnectedError
	}
	msgLen := 0
	for _, e := range msg {
		msgLen += len(e)
	}
	c.socketMu.Lock()
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(msgLen))
	if _, err := c.buffer.Write(header); err != nil {
		c.Close()
		c.socketMu.Unlock()
		return err
	}
	// write chunks one by one
	for _, e := range msg {
		if _, err := c.buffer.Write(e); err != nil {
			c.Close()
			c.socketMu.Unlock()
			return err
		}
	}
	// flush the buffer
	if c.minFlushPeriod <= 0 ||
		time.Now().After(c.lastFlush.Add(c.minFlushPeriod)) {
		if err := c.buffer.Flush(); err != nil {
			c.Close()
			c.socketMu.Unlock()
			return err
		}
		c.lastFlush = time.Now()
	}
	c.socketMu.Unlock()
	return nil
}

// Close connection.
func (c *MsgConn) Close() {
	c.socket.Close()
	c.closed = true
	if c.onDisconnect != nil {
		c.onDisconnect()
	}
}

// Return truth if connection is already closed.
func (c *MsgConn) Closed() bool {
	return c.closed
}

// Goroutine.
// Receive incoming messages from the other side
// and call callback function for each.
func (c *MsgConn) readLoop() {
	defer c.Close()
	for {
		packet, err := c.readPacket()
		if err != nil {
			return
		}
		c.handler(packet)
	}
}

// Receive next message from the other side.
func (c *MsgConn) readPacket() ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.socket, header); err != nil {
		return nil, err
	}
	len := int(binary.BigEndian.Uint32(header))
	if 0 < c.MaxPacketLen && c.MaxPacketLen < len {
		return nil, MsgTooLongError
	}
	buffer := make([]byte, len)
	if _, err := io.ReadFull(c.socket, buffer); err != nil {
		return nil, err
	}
	return buffer, nil
}
