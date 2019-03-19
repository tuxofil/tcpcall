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
	"tcpcall/proto"
	"time"
)

// Errors
var (
	MsgConnNotConnectedError = errors.New("msg conn: not connected")
	MsgTooLongError          = errors.New("incoming message is too long")
	MsgUnknownError          = errors.New("unknown incoming message")
)

// Message oriented connection
type MsgConn struct {
	// Configuration used to create MsgConn instance
	config    MsgConnConf
	socket    net.Conn
	buffer    *bufio.Writer
	lastFlush time.Time
	// socket write mutex
	socketMu sync.Mutex
}

type MsgConnConf struct {
	// Socket write buffer size, in bytes
	WriteBufferSize int
	// Maximum allowed length of incoming packets, in bytes
	MaxPacketLen int
	// Mandatory incoming package handler
	Handler func(proto.Packet)
	// Optional callback for disconnect event
	OnDisconnect func()
	// Minimum time between write buffer flushes
	MinFlushPeriod time.Duration
	// Optional interface to allocate byte slices for
	// incoming packets. Allocator must return a slice of
	// exact size given as the argument.
	Allocator func(int) []byte
}

// Create new message oriented connection.
func NewMsgConn(socket net.Conn, config MsgConnConf) (*MsgConn, error) {
	if err := socket.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if config.Allocator == nil {
		config.Allocator = func(n int) []byte {
			return make([]byte, n)
		}
	}
	conn := &MsgConn{
		config: config,
		socket: socket,
		buffer: bufio.NewWriterSize(socket, config.WriteBufferSize),
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
func (c *MsgConn) Send(packet proto.Packet) error {
	if c == nil {
		return MsgConnNotConnectedError
	}
	c.socketMu.Lock()
	if _, err := packet.WriteTo(c.buffer); err != nil {
		c.closeUnsafe()
		c.socketMu.Unlock()
		return err
	}
	// flush the buffer
	if c.config.MinFlushPeriod <= 0 ||
		time.Now().After(c.lastFlush.Add(c.config.MinFlushPeriod)) {
		if err := c.buffer.Flush(); err != nil {
			c.closeUnsafe()
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
	c.socketMu.Lock()
	c.closeUnsafe()
	c.socketMu.Unlock()
}

func (c *MsgConn) closeUnsafe() {
	if c.socket == nil {
		return
	}
	c.socket.Close()
	c.socket = nil
	if c.config.OnDisconnect != nil {
		c.config.OnDisconnect()
	}
}

// Return truth if connection is already closed.
func (c *MsgConn) Closed() bool {
	if c == nil {
		return true
	}
	c.socketMu.Lock()
	res := c.socket == nil
	c.socketMu.Unlock()
	return res
}

// Goroutine.
// Receive incoming messages from the other side
// and call callback function for each.
func (c *MsgConn) readLoop() {
	for {
		packet, err := c.readPacket()
		if err != nil {
			c.Close()
			return
		}
		c.config.Handler(packet)
	}
}

// Receive next message from the other side.
func (c *MsgConn) readPacket() (p proto.Packet, err error) {
	socket := c.socket
	if socket == nil {
		err = MsgConnNotConnectedError
		return
	}
	var h1 [5]byte // packet len and type
	err = readFull(socket, h1[:])
	if err != nil {
		return
	}
	packetLen := int(binary.BigEndian.Uint32(h1[:]))
	p.Type = int(h1[4])
	if m := c.config.MaxPacketLen; 0 < m && m < packetLen {
		err = MsgTooLongError
		return
	}
	// read rest of packet (headers and payload)
	switch p.Type {
	case proto.REQUEST:
		p.Data, err = c.readPacket2(socket, p.Headers[:], packetLen-13)
	case proto.REPLY:
		p.Data, err = c.readPacket2(socket, p.Headers[:4], packetLen-5)
	case proto.ERROR:
		p.Data, err = c.readPacket2(socket, p.Headers[:4], packetLen-5)
	case proto.CAST:
		p.Data, err = c.readPacket2(socket, p.Headers[:4], packetLen-5)
	case proto.FLOW_CONTROL_SUSPEND:
		p.Data, err = c.readPacket2(socket, p.Headers[:8], 0)
	case proto.FLOW_CONTROL_RESUME:
	case proto.UPLINK_CAST:
		p.Data, err = c.readPacket2(socket, nil, packetLen-1)
	default:
		err = MsgUnknownError
	}
	return
}

func (c *MsgConn) readPacket2(reader io.Reader, h2 []byte, dataLen int) ([]byte, error) {
	if 0 < len(h2) {
		if err := readFull(reader, h2); err != nil {
			return nil, err
		}
	}
	if dataLen == 0 {
		return nil, nil
	}
	buffer := c.config.Allocator(dataLen)
	if err := readFull(reader, buffer); err != nil {
		return nil, err
	}
	return buffer, nil
}

func readFull(reader io.Reader, buf []byte) error {
	_, err := io.ReadAtLeast(reader, buf, len(buf))
	return err
}
