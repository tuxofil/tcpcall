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
	"tcpcall/pools"
	"tcpcall/proto"
	"time"
)

// Client counter indices.
// See Client.counters field and Client.Counters()
// method description for details.
const (
	// How many times Req*() methods were called
	CC_REQUESTS = iota
	// How many times requests failed due to connection
	// concurrency overrun
	CC_OVERLOADS
	// How many times requests were not sent due to network errors
	CC_REQUEST_SEND_FAILS
	// How many valid replies were received for
	// sent requests.
	CC_REPLIES
	// How many times requests were timeouted waiting
	// for reply
	CC_TIMEOUTS
	// How many times Cast*() methods were called
	CC_CASTS
	// How many times casts were not sent due to network errors
	CC_CAST_SEND_FAILS
	// How many times client was connected to the server
	CC_CONNECTS
	// How many times client was disconnected from the server
	CC_DISCONNECTS
	// Count of invalid packets received
	CC_BAD_PACKETS
	// Count of reply packets received
	CC_REPLY_PACKETS
	// Count of reply packets with error reason received
	CC_ERROR_PACKETS
	// Count of suspend packets received
	CC_SUSPEND_PACKETS
	// Count of resume packets received
	CC_RESUME_PACKETS
	// Count of uplink cast packets received
	CC_UCAST_PACKETS
	CC_COUNT // special value - count of all counters
)

type RRegistry map[uint32]*RREntry

type RREntry struct {
	Deadline time.Time
	Chan     chan RRReply
}

type RRReply struct {
	Reply []byte
	Error []byte
}

// Connection state.
type Client struct {
	// address of the remote server to connect to
	peer string
	// client configuration
	config ClientConf
	// list of issued pending requests
	registry   RRegistry
	registryMu sync.Mutex
	// message oriented network socket
	socket   *MsgConn
	socketMu sync.RWMutex
	// channel for disconnection events
	closeChan chan bool
	// set to truth on client termination
	closed   bool
	closedMu sync.Mutex
	// Counters array
	counters   []int
	countersMu sync.RWMutex
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
	// Optional interface to allocate byte slices for
	// input requests and casts. Allocator must return a slice of
	// exact size given as the argument.
	Allocator func(int) []byte
}

// Client instance information returned by Info() method.
// Added mostly for debugging.
type ClientInfo struct {
	// Configuration used to create Client instance
	Config ClientConf
	// Remote side address and port number
	Peer string
	// Truth if client is currently connected to the server
	Connected bool
	// Counters array
	Counters []int
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

var (
	// gReplyPool is a pool of channels for replies.
	// Used to decrease allocations.
	gReplyPool = sync.Pool{New: func() interface{} {
		return make(chan RRReply, 1)
	}}
)

// Connect to server side.
// Return non nil error only when ClientConf.SyncConnect
// option is set to truth and initial connection failed.
// Regardless error value, returned Client instance is valid
// for further operation. It will connect to the server
// eventually (of course, if server will became available with time).
// If you dont want to use created Client instance in case
// when error != nil, you must call Close() method.
func Dial(dst string, conf ClientConf) (*Client, error) {
	c := &Client{
		peer:      dst,
		config:    conf,
		registry:  RRegistry{},
		closeChan: make(chan bool, 50),
		counters:  make([]int, CC_COUNT),
	}
	var err error
	if conf.SyncConnect {
		err = c.connect()
	}
	go c.connectLoop()
	return c, err
}

// Create default client configuration
func NewClientConf() ClientConf {
	return ClientConf{
		Concurrency:     gConcurrency,
		ReconnectPeriod: time.Millisecond * 100,
		MinFlushPeriod:  gMinFlushPeriod,
		WriteBufferSize: gWriteBufferSize,
		SyncConnect:     true,
		Trace:           gTraceClient,
	}
}

// Make synchronous request to the server.
func (c *Client) ReqChunks(chunks [][]byte, timeout time.Duration) ([]byte, error) {
	return c.Req(bytes.Join(chunks, nil), timeout)
}

// Make synchronous request to the server.
func (c *Client) Req(data []byte, timeout time.Duration) ([]byte, error) {
	c.hit(CC_REQUESTS)
	entry := &RREntry{
		Deadline: time.Now().Add(timeout),
		Chan:     gReplyPool.Get().(chan RRReply),
	}
	if len(entry.Chan) > 0 {
		<-entry.Chan
	}
	seqNum := proto.GenSeqNum()
	// queue
	c.registryMu.Lock()
	if c.config.Concurrency <= len(c.registry) {
		c.registryMu.Unlock()
		c.hit(CC_OVERLOADS)
		gReplyPool.Put(entry.Chan)
		return nil, OverloadError
	}
	// as far as seqnum is uint32, we'll do no checks
	// for seqnum collision here. It's unlikely someone
	// will use concurrency greater than 2^32 to make
	// such collisions possible.
	c.registry[seqNum] = entry
	c.registryMu.Unlock()
	defer c.popRegistry(seqNum)
	// send through the network
	packet := proto.NewRequest(seqNum, entry.Deadline, data)
	if err := c.socketSend(packet); err != nil {
		c.hit(CC_REQUEST_SEND_FAILS)
		gReplyPool.Put(entry.Chan)
		if err == MsgConnNotConnectedError {
			return nil, NotConnectedError
		}
		return nil, DisconnectedError
	}
	c.log("req sent")
	// wait for the response
	after := pools.GetFreeTimer(entry.Deadline.Sub(time.Now()))
	select {
	case reply := <-entry.Chan:
		c.hit(CC_REPLIES)
		gReplyPool.Put(entry.Chan)
		if reply.Error == nil {
			pools.ReleaseTimer(after)
			return reply.Reply, nil
		}
		pools.ReleaseTimer(after)
		return nil, RemoteCrashedError
	case <-after.C:
		c.hit(CC_TIMEOUTS)
		gReplyPool.Put(entry.Chan)
		return nil, TimeoutError
	}
}

func (c *Client) socketSend(packet proto.Packet) error {
	c.socketMu.RLock()
	socket := c.socket
	c.socketMu.RUnlock()
	return socket.Send(packet)
}

// Make asynchronous request to the server.
func (c *Client) CastChunks(chunks [][]byte) error {
	return c.Cast(bytes.Join(chunks, nil))
}

// Make asynchronous request to the server.
func (c *Client) Cast(data []byte) error {
	c.hit(CC_CASTS)
	packet := proto.NewCast(proto.GenSeqNum(), data)
	if err := c.socketSend(packet); err != nil {
		c.hit(CC_CAST_SEND_FAILS)
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

// Return client's info and stats.
func (c *Client) Info() ClientInfo {
	return ClientInfo{
		Config:    c.config,
		Peer:      c.peer,
		Connected: !c.socket.Closed(),
		Counters:  c.Counters(),
	}
}

// Return a snapshot of all internal counters.
func (c *Client) Counters() []int {
	res := make([]int, CC_COUNT)
	c.countersMu.RLock()
	copy(res, c.counters)
	c.countersMu.RUnlock()
	return res
}

// Thread safe counter increment.
func (c *Client) hit(counter int) {
	c.countersMu.Lock()
	c.counters[counter]++
	c.countersMu.Unlock()
}

// Connect (or reconnect) to the server.
func (c *Client) connect() error {
	c.disconnect()
	conn, err := net.Dial("tcp", c.peer)
	if err == nil {
		c.counters[CC_CONNECTS]++
		c.log("connected")
		msgConn, err := NewMsgConn(conn, MsgConnConf{
			MinFlushPeriod:  c.config.MinFlushPeriod,
			WriteBufferSize: c.config.WriteBufferSize,
			MaxPacketLen:    c.config.MaxReplySize,
			Handler:         c.handlePacket,
			OnDisconnect:    c.notifyClose,
			Allocator:       c.config.Allocator,
		})
		if err != nil {
			return err
		}
		c.socketMu.Lock()
		c.socket = msgConn
		c.socketMu.Unlock()
		c.notifyPool(true)
	} else {
		c.log("failed to connect: %s", err)
	}
	return err
}

// Terminate the client.
func (c *Client) Close() {
	c.log("closing...")
	c.closedMu.Lock()
	c.closed = true
	c.closedMu.Unlock()
	c.disconnect()
	c.log("closed")
}

// Close connection to server.
func (c *Client) disconnect() {
	if c.socket.Closed() {
		return
	}
	c.socket.Close()
	c.notifyClose()
	// discard all pending requests
	c.registryMu.Lock()
	for _, entry := range c.registry {
		select {
		case entry.Chan <- RRReply{nil, []byte("disconnected")}:
		default:
		}
	}
	c.registry = RRegistry{}
	c.registryMu.Unlock()
	c.log("disconnected")
	c.counters[CC_DISCONNECTS]++
}

// Goroutine.
// Reconnects on network errors.
func (c *Client) connectLoop() {
	c.log("daemon started")
	defer c.log("daemon terminated")
	for {
		if c.Closed() {
			return
		}
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

// Return true if client is terminated.
func (c *Client) Closed() bool {
	c.closedMu.Lock()
	closed := c.closed
	c.closedMu.Unlock()
	return closed
}

// Send connection state change notification to Client owner
func (c *Client) notifyPool(connected bool) {
	if c.config.StateListener != nil && !c.Closed() {
		after := pools.GetFreeTimer(time.Second / 5)
		select {
		case c.config.StateListener <- StateEvent{c, connected}:
		case <-after.C:
		}
		pools.ReleaseTimer(after)
	}
}

// Callback for message-oriented socket.
// Handle message received from the remote peer.
func (c *Client) handlePacket(packet proto.Packet) {
	c.log("decoded packet_type=%d; data=%v",
		packet.Type, packet.Data)
	switch packet.Type {
	case proto.REPLY:
		c.counters[CC_REPLY_PACKETS]++
		if entry, ok := c.popRegistry(packet.SeqNum()); ok {
			entry.Chan <- RRReply{Reply: packet.Data}
		}
	case proto.ERROR:
		c.counters[CC_ERROR_PACKETS]++
		if entry, ok := c.popRegistry(packet.SeqNum()); ok {
			entry.Chan <- RRReply{Error: packet.Data}
		}
	case proto.FLOW_CONTROL_SUSPEND:
		c.counters[CC_SUSPEND_PACKETS]++
		if c.config.SuspendListener != nil {
			c.config.SuspendListener <- SuspendEvent{
				Sender:   c,
				Duration: packet.SuspendDuration(),
			}
		}
	case proto.FLOW_CONTROL_RESUME:
		c.counters[CC_RESUME_PACKETS]++
		if c.config.ResumeListener != nil {
			c.config.ResumeListener <- ResumeEvent{c}
		}
	case proto.UPLINK_CAST:
		c.counters[CC_UCAST_PACKETS]++
		if c.config.UplinkCastListener != nil {
			c.config.UplinkCastListener <- UplinkCastEvent{
				Sender: c,
				Data:   packet.Data,
			}
		}
	}
}

// Lookup request in the registry and remove it.
func (c *Client) popRegistry(seqnum uint32) (e *RREntry, ok bool) {
	c.registryMu.Lock()
	e, ok = c.registry[seqnum]
	if ok {
		delete(c.registry, seqnum)
	}
	c.registryMu.Unlock()
	return
}

// Print message to the stdout if verbose mode is enabled.
func (c *Client) log(format string, args ...interface{}) {
	if c.config.Trace {
		prefix := fmt.Sprintf("tcpcall conn %s> ", c.peer)
		log.Printf(prefix+format, args...)
	}
}
