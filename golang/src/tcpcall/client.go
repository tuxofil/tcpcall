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
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"tcpcall/proto"
	"time"
)

type registry map[proto.SeqNum]*registryEntry

type registryEntry struct {
	Deadline time.Time
	Chan     chan reply
}

type reply struct {
	Reply []byte
	Error []byte
}

// Connection state.
type Client struct {
	peer   string
	config ClientConf
	registry
	lock      *sync.Mutex
	socket    net.Conn
	stop_flag bool
}

// Connection configuration.
type ClientConf struct {
	// Maximum parallel requests for the connection.
	Concurrency int
	// Sleep duration before reconnect after connection failure.
	ReconnectPeriod time.Duration
	// Max reply packet size, in bytes. 0 means no limit.
	MaxReplySize int
	// Channel to send state events (connected/disconnected).
	StateListener *chan StateEvent
	// Channel to send 'suspend' events.
	SuspendListener *chan SuspendEvent
	// Channel to send 'resume' events.
	ResumeListener *chan ResumeEvent
	// Channel to send Uplink Cast data.
	UplinkCastListener *chan UplinkCastEvent
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
		peer:     dst,
		config:   conf,
		registry: make(registry, conf.Concurrency),
		lock:     &sync.Mutex{},
	}
	if conf.SyncConnect {
		err = c.connect()
	}
	go c.startClientDaemon()
	return c, err
}

// Create default client configuration
func NewClientConf() ClientConf {
	return ClientConf{
		Concurrency:     1000,
		ReconnectPeriod: time.Millisecond * 100,
		SyncConnect:     true,
		Trace:           trace_client,
	}
}

// Make synchronous request to the server.
func (c *Client) Req(data []byte, timeout time.Duration) (rep []byte, err error) {
	deadline := time.Now().Add(timeout)
	req := proto.NewRequest(data, deadline)
	encoded := req.Encode()
	//
	backchan, err := c.queue(req.SeqNum, encoded, deadline)
	if err != nil {
		return nil, err
	}
	// wait for the response
	select {
	case reply := <-backchan:
		if reply.Error == nil {
			return reply.Reply, nil
		}
		return nil, RemoteCrashedError{string(reply.Error)}
	case <-time.After(deadline.Sub(time.Now())):
		c.popRegistry(req.SeqNum)
		return nil, TimeoutError{}
	}
}

// Make asynchronous request to the server.
func (c *Client) Cast(data []byte) error {
	encoded := proto.NewCast(data).Encode()
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(encoded)))
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		return NotConnectedError{}
	}
	_, err := c.socket.Write(header)
	if err != nil {
		c.closeUnsafe()
		return DisconnectedError{}
	}
	_, err = c.socket.Write(encoded)
	if err != nil {
		c.closeUnsafe()
		return DisconnectedError{}
	}
	c.log("cast sent")
	return nil
}

// Enqueue new request to the server.
func (c *Client) queue(seqnum proto.SeqNum, data []byte, deadline time.Time) (chan reply, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		return nil, NotConnectedError{}
	}
	if c.config.Concurrency <= len(c.registry) {
		return nil, OverloadError{}
	}
	backchan := make(chan reply, 1)
	c.registry[seqnum] = &registryEntry{deadline, backchan}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	_, err := c.socket.Write(header)
	if err != nil {
		c.closeUnsafe()
		return nil, DisconnectedError{}
	}
	_, err = c.socket.Write(data)
	if err != nil {
		c.closeUnsafe()
		return nil, DisconnectedError{}
	}
	c.log("req sent")
	return backchan, nil
}

// GetQueuedRequests function return total count of requests being
// processed right now.
func (c *Client) GetQueuedRequests() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.registry)
}

// Connect (or reconnect) to the server.
func (c *Client) connect() error {
	c.lock.Lock()
	err := c.connectUnsafe()
	c.lock.Unlock()
	return err
}

// Connect (or reconnect) to the server.
func (c *Client) connectUnsafe() error {
	c.closeUnsafe()
	conn, err := net.Dial("tcp", c.peer)
	if err == nil {
		c.log("connected")
		c.socket = conn
		if c.config.StateListener != nil {
			(*c.config.StateListener) <- StateEvent{c, true}
		}
	} else {
		c.log("failed to connect: %s", err)
	}
	return err
}

// Close connection to server.
func (c *Client) Close() {
	c.log("closing...")
	c.stop_flag = true
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closeUnsafe()
	c.log("closed")
}

// Close connection to server.
func (c *Client) closeUnsafe() {
	s := c.socket
	c.socket = nil
	if s != nil {
		if c.config.StateListener != nil && !c.stop_flag {
			select {
			case (*c.config.StateListener) <- StateEvent{c, true}:
			case <-time.After(time.Second / 5):
			}
		}
		s.Close()
		// discard all pending requests
		for _, v := range c.registry {
			select {
			case v.Chan <- reply{nil, []byte("disconnected")}:
			default:
			}
		}
		c.registry = make(registry, c.config.Concurrency)
		c.log("disconnected")
	}
}

// Goroutine.
// Receives data from network and dispatch them to callers.
// Reconnects on network errors.
func (c *Client) startClientDaemon() {
	c.log("daemon started")
	defer c.log("daemon terminated")
	for !c.stop_flag {
		if c.socket != nil {
			ptype, data, err := c.readPacket()
			if err != nil {
				c.lock.Lock()
				c.closeUnsafe()
				c.lock.Unlock()
				continue
			}
			switch ptype {
			case proto.REPLY:
				p := data.(*proto.PacketReply)
				regEntry := c.popRegistry(p.SeqNum)
				if regEntry != nil {
					regEntry.Chan <- reply{p.Reply, nil}
				}
			case proto.ERROR:
				p := data.(*proto.PacketError)
				regEntry := c.popRegistry(p.SeqNum)
				if regEntry != nil {
					regEntry.Chan <- reply{nil, p.Reason}
				}
			case proto.FLOW_CONTROL_SUSPEND:
				if c.config.SuspendListener != nil {
					p := data.(*proto.PacketFlowControlSuspend)
					(*c.config.SuspendListener) <- SuspendEvent{c, p.Duration}
				}
			case proto.FLOW_CONTROL_RESUME:
				if c.config.ResumeListener != nil {
					(*c.config.ResumeListener) <- ResumeEvent{c}
				}
			case proto.UPLINK_CAST:
				if c.config.UplinkCastListener != nil {
					p := data.(*proto.PacketUplinkCast)
					(*c.config.UplinkCastListener) <- UplinkCastEvent{c, p.Data}
				}
			}
		} else {
			if err := c.connect(); err != nil {
				time.Sleep(c.config.ReconnectPeriod)
			}
		}
	}
}

// Read and decode next packet from the server.
func (c *Client) readPacket() (int, interface{}, error) {
	header := make([]byte, 4)
	n, err := c.socket.Read(header)
	if err != nil || n != 4 {
		// failed to read packet header
		return -1, nil, err
	}
	plen := binary.BigEndian.Uint32(header)
	c.log("packet len=%d", plen)
	if c.config.MaxReplySize != 0 && c.config.MaxReplySize < int(plen) {
		return -1, nil, fmt.Errorf("reply packet too long (%d bytes)", plen)
	}
	bytes := make([]byte, plen)
	n, err = c.socket.Read(bytes)
	if err != nil || uint32(n) != plen {
		// failed to read packet body
		return -1, nil, err
	}
	ptype, data, err := proto.Decode(bytes)
	c.log("decoded packet_type=%d; data=%v; err=%v", ptype, data, err)
	if err != nil {
		c.log("decode failed: %v", err)
		return -1, nil, err
	}
	return ptype, data, nil
}

// Lookup request in the registry and remove it.
func (c *Client) popRegistry(seqnum proto.SeqNum) *registryEntry {
	c.lock.Lock()
	defer c.lock.Unlock()
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
