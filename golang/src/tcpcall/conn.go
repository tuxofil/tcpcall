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
	socket net.Conn
	// socket write mutex
	socketMu sync.Locker
	// maximum allowed length for incoming packets
	MaxPacketLen int
	// incoming package handler
	handler func([]byte)
	// callback for disconnect event
	onDisconnect func()
	closed       bool
}

// Create new message oriented connection.
func NewMsgConn(socket net.Conn, handler func([]byte), onClose func()) (*MsgConn, error) {
	if err := socket.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	conn := &MsgConn{
		socket:       socket,
		socketMu:     &sync.Mutex{},
		handler:      handler,
		onDisconnect: onClose,
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
func (c *MsgConn) Send(msg []byte) error {
	if c == nil {
		return MsgConnNotConnectedError
	}
	c.socketMu.Lock()
	defer c.socketMu.Unlock()
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(msg)))
	if _, err := c.socket.Write(header); err != nil {
		c.Close()
		return err
	}
	if _, err := c.socket.Write(msg); err != nil {
		c.Close()
		return err
	}
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
