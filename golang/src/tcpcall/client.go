/*
Client connection.

Allow to make simultaneous requests through the one TCP connection.
Reconnects to the server on network failures.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"tcpcall/proto"
	"time"
)

type RRegistry map[proto.SeqNum]*RREntry

type RREntry struct {
	Deadline time.Time
	Chan     chan RRReply
}

type RRReply struct {
	Reply [][]byte
	Error [][]byte
}

// Connection state.
type Client struct {
	// address of the remote server to connect to
	peer string
	// client configuration
	config ClientConf
	// list of issued pending requests
	registry   RRegistry
	registryMu sync.Locker
	// message oriented network socket
	socket *MsgConn
	// channel for disconnection events
	closeChan chan bool
	// set to truth on client termination
	closed bool
}

// Connection configuration.
type ClientConf struct {
	// Maximum parallel requests for the connection.
	Concurrency int
	// Sleep duration before reconnect after connection failure.
	ReconnectPeriod time.Duration
	// Max reply packet size, in bytes. 0 means no limit.
	MaxReplySize int
	// Minimum flush period for socket writer
	MinFlushPeriod time.Duration
	// Socket write buffer size
	WriteBufferSize int
	// Channel to send state events (connected/disconnected).
	StateListener chan StateEvent
	// Channel to send 'suspend' events.
	SuspendListener chan SuspendEvent
	// Channel to send 'resume' events.
	ResumeListener chan ResumeEvent
	// Channel to send Uplink Cast data.
	UplinkCastListener chan UplinkCastEvent
	// If true, Dial() function will attempt to connect to the
	// server before returning. Default is true.
	SyncConnect bool
	// Enable default logging or not.
	Trace bool
}

// Connection state event.
type StateEvent struct {
	// Pointer to the client connection state.
	Sender *Client
	// If true - client just have been connected to the server.
	// If false - disconnected.
	Online bool
}

// Sent when 'suspend' signal from server received.
type SuspendEvent struct {
	// Pointer to the client connection state.
	Sender *Client
	// Requested suspend duration
	Duration time.Duration
}

// Sent when 'resume' signal from server received.
type ResumeEvent struct {
	// Pointer to the client connection state.
	Sender *Client
}

// Sent when uplink cast data received from server.
type UplinkCastEvent struct {
	// Pointer to the client connection state.
	Sender *Client
	Data   []byte
}

// Connect to server side.
func Dial(dst string, conf ClientConf) (c *Client, err error) {
	c = &Client{
		peer:       dst,
		config:     conf,
		registry:   RRegistry{},
		registryMu: &sync.Mutex{},
		closeChan:  make(chan bool, 50),
	}
	if conf.SyncConnect {
		err = c.connect()
	}
	go c.connectLoop()
	return c, err
}

// Create default client configuration
func NewClientConf() ClientConf {
	return ClientConf{
		Concurrency:     defConcurrency,
		ReconnectPeriod: time.Millisecond * 100,
		MinFlushPeriod:  defMinFlush,
		WriteBufferSize: defWBufSize,
		SyncConnect:     true,
		Trace:           traceClient,
	}
}

// Make synchronous request to the server.
func (c *Client) Req(payload []byte, timeout time.Duration) (rep []byte, err error) {
	return c.ReqChunks([][]byte{payload}, timeout)
}

// Make synchronous request to the server.
func (c *Client) ReqChunks(payload [][]byte, timeout time.Duration) (rep []byte, err error) {
	// queue
	c.registryMu.Lock()
	if c.config.Concurrency <= len(c.registry) {
		c.registryMu.Unlock()
		return nil, OverloadError
	}
	entry := &RREntry{
		Deadline: time.Now().Add(timeout),
		Chan:     make(chan RRReply, 1),
	}
	req := proto.NewRequest(payload, entry.Deadline)
	encoded := req.Encode()
	c.registry[req.SeqNum] = entry
	c.registryMu.Unlock()
	// send through the network
	if err := c.socket.Send(encoded); err != nil {
		c.popRegistry(req.SeqNum)
		if err == MsgConnNotConnectedError {
			return nil, NotConnectedError
		}
		return nil, DisconnectedError
	}
	c.log("req sent")
	// wait for the response
	select {
	case reply := <-entry.Chan:
		if reply.Error == nil {
			return bytes.Join(reply.Reply, []byte{}), nil
		}
		return nil, RemoteCrashedError
	case <-time.After(entry.Deadline.Sub(time.Now())):
		c.popRegistry(req.SeqNum)
		return nil, TimeoutError
	}
}

// Make asynchronous request to the server.
func (c *Client) Cast(data []byte) error {
	return c.CastChunks([][]byte{data})
}

// Make asynchronous request to the server.
func (c *Client) CastChunks(data [][]byte) error {
	encoded := proto.NewCast(data).Encode()
	if err := c.socket.Send(encoded); err != nil {
		return err
	}
	c.log("cast sent")
	return nil
}

// GetQueuedRequests function return total count of requests being
// processed right now.
func (c *Client) GetQueuedRequests() int {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	return len(c.registry)
}

// Connect (or reconnect) to the server.
func (c *Client) connect() error {
	c.disconnect()
	conn, err := net.Dial("tcp", c.peer)
	if err == nil {
		c.log("connected")
		msgConn, err := NewMsgConn(conn, c.config.MinFlushPeriod,
			c.config.WriteBufferSize,
			c.handlePacket, c.notifyClose)
		if err != nil {
			return err
		}
		msgConn.MaxPacketLen = c.config.MaxReplySize
		c.socket = msgConn
		c.notifyPool(true)
	} else {
		c.log("failed to connect: %s", err)
	}
	return err
}

// Terminate the client.
func (c *Client) Close() {
	c.log("closing...")
	c.closed = true
	c.disconnect()
	c.log("closed")
}

// Close connection to server.
func (c *Client) disconnect() {
	if c.socket == nil || c.socket.Closed() {
		return
	}
	c.socket.Close()
	c.notifyClose()
	// discard all pending requests
	c.registryMu.Lock()
	for _, entry := range c.registry {
		select {
		case entry.Chan <- RRReply{nil, [][]byte{[]byte("disconnected")}}:
		default:
		}
	}
	c.registry = RRegistry{}
	c.registryMu.Unlock()
	c.log("disconnected")
}

// Goroutine.
// Reconnects on network errors.
func (c *Client) connectLoop() {
	c.log("daemon started")
	defer c.log("daemon terminated")
	for !c.closed {
		if c.socket == nil || c.socket.Closed() {
			if err := c.connect(); err != nil {
				time.Sleep(c.config.ReconnectPeriod)
				continue
			}
		}
		<-c.closeChan
	}
}

// Send 'connection closed' notification to the client daemon.
func (c *Client) notifyClose() {
	c.notifyPool(false)
	c.closeChan <- true
}

// Send connection state change notification to Client owner
func (c *Client) notifyPool(connected bool) {
	if c.config.StateListener != nil && !c.closed {
		select {
		case c.config.StateListener <- StateEvent{c, connected}:
		case <-time.After(time.Second / 5):
		}
	}
}

// Callback for message-oriented socket.
// Handle message received from the remote peer.
func (c *Client) handlePacket(packet []byte) {
	ptype, payload, err := proto.Decode(packet)
	c.log("decoded packet_type=%d; data=%v; err=%s", ptype, payload, err)
	if err != nil {
		// close connection on bad packet receive
		c.log("decode failed: %s", err)
		c.disconnect()
		return
	}
	switch ptype {
	case proto.REPLY:
		p := payload.(*proto.PacketReply)
		if entry := c.popRegistry(p.SeqNum); entry != nil {
			entry.Chan <- RRReply{p.Reply, nil}
		}
	case proto.ERROR:
		p := payload.(*proto.PacketError)
		if entry := c.popRegistry(p.SeqNum); entry != nil {
			entry.Chan <- RRReply{nil, p.Reason}
		}
	case proto.FLOW_CONTROL_SUSPEND:
		if c.config.SuspendListener != nil {
			p := payload.(*proto.PacketFlowControlSuspend)
			c.config.SuspendListener <- SuspendEvent{c, p.Duration}
		}
	case proto.FLOW_CONTROL_RESUME:
		if c.config.ResumeListener != nil {
			c.config.ResumeListener <- ResumeEvent{c}
		}
	case proto.UPLINK_CAST:
		if c.config.UplinkCastListener != nil {
			p := payload.(*proto.PacketUplinkCast)
			flat := bytes.Join(p.Data, []byte{})
			c.config.UplinkCastListener <- UplinkCastEvent{c, flat}
		}
	}
}

// Lookup request in the registry and remove it.
func (c *Client) popRegistry(seqnum proto.SeqNum) *RREntry {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	res := c.registry[seqnum]
	if res != nil {
		delete(c.registry, seqnum)
	}
	return res
}

// Print message to the stdout if verbose mode is enabled.
func (c *Client) log(format string, args ...interface{}) {
	if c.config.Trace {
		prefix := fmt.Sprintf("tcpcall conn %s> ", c.peer)
		log.Printf(prefix+format, args...)
	}
}
