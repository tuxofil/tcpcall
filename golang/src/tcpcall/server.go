/*
Server.

Accepts client connections. Runs predefined processing function
for each incoming request and send the answer back to the client side.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"fmt"
	"log"
	"net"
	"sync"
	"tcpcall/proto"
	"time"
)

// Server counter indices.
const (
	// when new TCP connection established
	SC_ACCEPTED = iota
	// failed to accept new incoming connection
	SC_ACCEPT_ERRORS
	// too many incoming connections already exist
	SC_ACCEPT_OVERFLOWS
	// client connection handler creation error
	SC_HANDLER_CREATE_ERRORS
	// new packet received from the client
	SC_PACKET_INPUT
	// too many request processors (incoming packet
	// will be dropped without processing)
	SC_CONCURRENCY_OVERFLOWS
	// we're about sending suspend signal to the client
	SC_SUSPEND_REQUESTED
	// suspend signal not sent due to error
	SC_SUSPEND_REQUEST_ERRORS
	// we're about sending resume signal to the client
	SC_RESUME_REQUESTED
	// resume signal not sent due to error
	SC_RESUME_REQUEST_ERRORS
	// we're about sending packet back to the client
	SC_PACKET_WRITE
	// packet not sent due to error
	SC_PACKET_WRITE_ERRORS
	// new request processing worker (goroutine) created
	SC_WORKER_ADDED
	// request processing worker (goroutine) finished
	SC_WORKER_REMOVED
	// failed to decode packet received from the client
	SC_PACKET_DECODE_ERRORS
	// sync request received
	SC_REQUESTS
	// async request received
	SC_CASTS
	// unknown request received (and left without processing)
	SC_UNKNOWN
	// we're about sending uplink cast packet to the client
	SC_UPLINK_CAST_REQUESTED
	// uplink cast packet not sent due to error
	SC_UPLINK_CAST_ERRORS
	SC_COUNT // special value - count of all counters
)

// Server state
type Server struct {
	// IP address and TCP port number used for listening
	// incoming connections (colon separated).
	bindAddr string
	// Configuration used for server instance creation.
	config ServerConf
	// Server socket.
	socket net.Listener
	// List of established client connections.
	connections map[*ServerConn]struct{}
	// Controls access to client connections map.
	lock sync.RWMutex
	// Set to truth when server is about to terminate.
	stopFlag bool
	// Counters array
	counters   []int
	countersMu sync.RWMutex
}

// Server configuration
type ServerConf struct {
	// TCP port number to listen
	PortNumber uint16
	// Maximum simultaneous connections to accept
	MaxConnections int
	// Maximum request processing concurrency per connection.
	Concurrency int
	// Max request packet size, in bytes. 0 means no limit.
	MaxRequestSize int
	// Minimum flush period for socket writer
	MinFlushPeriod time.Duration
	// Socket write buffer size
	WriteBufferSize int
	// Request processing function.
	// Each request will be processed in parallel with others.
	RequestCallback func([]byte) []byte
	// Asynchronous request (cast) processing function.
	// Each request will be processed in parallel with others.
	CastCallback func([]byte)
	// Called with encoded network packet when there are no workers
	// left to process the packet.
	OnDrop func([]byte)
	// Duration for suspend signals
	SuspendDuration time.Duration
	// Enable debug logging or not
	Trace bool
	// Optional interface to allocate byte slices for
	// input requests and casts. Allocator must return a slice of
	// exact size given as the argument.
	Allocator func(int) []byte
}

// Server instance information returned by Info() method.
// Added mostly for debugging.
type ServerInfo struct {
	// Configuration used to create Server instance
	Config ServerConf
	// Bind address and port number actually used
	BindAddr string
	// Truth means server is terminating (or terminated)
	StopFlag bool
	// Count of established connections from clients
	Connections int
	// Counters array
	Counters []int
}

// Connection handler state
type ServerConn struct {
	// IP address and TCP port number of the other
	// side of connection (client side).
	peer string
	// Message oriented connection
	conn *MsgConn
	// Number of currently running workers
	// (actually worker is a goroutine handling one
	// request received from the client).
	workers int
	// Controls access to conn object.
	lock sync.RWMutex
	// Link to the Server instance this conection
	// is originated from.
	server *Server
}

// Start new server.
func Listen(conf ServerConf) (*Server, error) {
	bindAddr := fmt.Sprintf(":%d", conf.PortNumber)
	socket, err := net.Listen("tcp", bindAddr)
	if err == nil {
		server := &Server{
			bindAddr:    bindAddr,
			config:      conf,
			socket:      socket,
			connections: map[*ServerConn]struct{}{},
			counters:    make([]int, SC_COUNT),
		}
		go server.acceptLoop()
		return server, nil
	}
	return nil, err
}

// Create new default configuration for server.
func NewServerConf() ServerConf {
	return ServerConf{
		MaxConnections:  500,
		Concurrency:     1000,
		SuspendDuration: time.Second,
		MinFlushPeriod:  gMinFlushPeriod,
		WriteBufferSize: gWriteBufferSize,
		Trace:           gTraceServer,
	}
}

// Stop the server.
func (s *Server) Stop() {
	s.lock.Lock()
	s.stopFlag = true
	for h, _ := range s.connections {
		go h.close()
	}
	s.lock.Unlock()
}

// Send 'suspend' signal to all connected clients.
func (s *Server) Suspend(duration time.Duration) {
	s.lock.Lock()
	for conn := range s.connections {
		conn.suspend(duration)
	}
	s.lock.Unlock()
}

// Send 'resume' signal to all connected clients.
func (s *Server) Resume() {
	s.lock.Lock()
	for conn := range s.connections {
		conn.resume()
	}
	s.lock.Unlock()
}

// Send uplink cast packet to all connected clients.
func (s *Server) UplinkCast(data []byte) {
	s.lock.Lock()
	for conn := range s.connections {
		conn.uplinkCast(data)
	}
	s.lock.Unlock()
}

// Goroutine.
// Accept incoming connections.
func (s *Server) acceptLoop() {
	defer s.socket.Close()
	s.log("daemon started")
	defer s.log("daemon terminated")
	for {
		if s.Stopped() {
			return
		}
		if s.config.MaxConnections <= s.Connections() {
			s.counters[SC_ACCEPT_OVERFLOWS]++
			time.Sleep(time.Millisecond * 200)
			continue
		}
		socket, err := s.socket.Accept()
		s.counters[SC_ACCEPTED]++
		if err != nil {
			s.counters[SC_ACCEPT_ERRORS]++
			time.Sleep(time.Millisecond * 200)
			continue
		}
		if s.Stopped() {
			socket.Close()
			return
		}
		h := &ServerConn{
			peer:   socket.RemoteAddr().String(),
			server: s,
		}
		msgConn, err := NewMsgConn(socket, MsgConnConf{
			MinFlushPeriod:  s.config.MinFlushPeriod,
			WriteBufferSize: s.config.WriteBufferSize,
			Handler:         h.onRecv,
			OnDisconnect:    h.onClose,
			Allocator:       s.config.Allocator,
		})
		if err != nil {
			s.counters[SC_HANDLER_CREATE_ERRORS]++
			socket.Close()
			continue
		}
		h.lock.Lock()
		h.conn = msgConn
		h.lock.Unlock()
		h.log("accepted")
		s.lock.Lock()
		s.connections[h] = struct{}{}
		s.lock.Unlock()
	}
}

func (s *Server) Connections() int {
	s.lock.RLock()
	res := len(s.connections)
	s.lock.RUnlock()
	return res
}

func (s *Server) Stopped() bool {
	s.lock.RLock()
	res := s.stopFlag
	s.lock.RUnlock()
	return res
}

// Return count of active client connections opened.
func (s *Server) GetConnections() int {
	s.lock.Lock()
	res := len(s.connections)
	s.lock.Unlock()
	return res
}

// Return total count of running requests.
func (s *Server) GetWorkers() int {
	s.lock.Lock()
	workers := 0
	for conn := range s.connections {
		workers += conn.workers
	}
	s.lock.Unlock()
	return workers
}

// Print message to the stdout if verbose mode is enabled.
func (s *Server) log(format string, args ...interface{}) {
	if s.config.Trace {
		prefix := fmt.Sprintf("tcpcall srv%s> ", s.bindAddr)
		log.Printf(prefix+format, args...)
	}
}

// Thread safe counter increment.
func (s *Server) hit(counter int) {
	s.countersMu.Lock()
	s.counters[counter]++
	s.countersMu.Unlock()
}

// Return snapshot of server internal counters.
func (s *Server) Counters() []int {
	res := make([]int, SC_COUNT)
	s.countersMu.RLock()
	copy(res, s.counters)
	s.countersMu.RUnlock()
	return res
}

// Return Server's info and stats.
func (s *Server) Info() ServerInfo {
	return ServerInfo{
		Config:      s.config,
		BindAddr:    s.bindAddr,
		StopFlag:    s.stopFlag,
		Connections: s.GetConnections(),
		Counters:    s.Counters(),
	}
}

// Callback for message-oriented socket.
// Handles connection close event.
func (h *ServerConn) onClose() {
	h.server.dropConn(h)
	h.log("connection closed")
}

// Safely remove connection from the state.
func (s *Server) dropConn(h *ServerConn) {
	s.lock.Lock()
	delete(s.connections, h)
	s.lock.Unlock()
}

// Callback for message-oriented socket.
// Handles incoming message from the remote side.
func (h *ServerConn) onRecv(packet proto.Packet) {
	h.server.hit(SC_PACKET_INPUT)
	h.lock.RLock()
	if h.server.config.Concurrency < h.workers {
		h.lock.RUnlock()
		// max workers count reached
		h.server.hit(SC_CONCURRENCY_OVERFLOWS)
		if f := h.server.config.OnDrop; f != nil {
			f(packet.Data)
		}
		if err := h.suspend(h.server.config.SuspendDuration); err != nil {
			h.log("suspend send: %v", err)
			h.close()
		}
		return
	}
	h.lock.RUnlock()
	h.incrementWorkers()
	go func() {
		h.log("got packet of type %d: %v", packet.Type, packet.Data)
		switch packet.Type {
		case proto.REQUEST:
			h.server.hit(SC_REQUESTS)
			h.processRequest(packet)
		case proto.CAST:
			h.server.hit(SC_CASTS)
			if f := h.server.config.CastCallback; f != nil {
				f(packet.Data)
			}
		default:
			// ignore packet
			h.server.hit(SC_UNKNOWN)
		}
		h.decrementWorkers()
	}()
}

// Send 'suspend' signal to the connected client.
func (h *ServerConn) suspend(duration time.Duration) error {
	h.server.hit(SC_SUSPEND_REQUESTED)
	if err := h.writePacket(proto.NewSuspend(duration)); err != nil {
		h.server.hit(SC_SUSPEND_REQUEST_ERRORS)
		return err
	}
	return nil
}

// Send 'resume' signal to the connected client.
func (h *ServerConn) resume() error {
	h.server.hit(SC_RESUME_REQUESTED)
	if err := h.writePacket(proto.NewResume()); err != nil {
		h.server.hit(SC_RESUME_REQUEST_ERRORS)
		return err
	}
	return nil
}

// Send uplink cast packet to client side.
func (h *ServerConn) uplinkCast(data []byte) error {
	h.server.hit(SC_UPLINK_CAST_REQUESTED)
	if err := h.writePacket(proto.NewUplinkCast(data)); err != nil {
		h.server.hit(SC_UPLINK_CAST_ERRORS)
		return err
	}
	return nil
}

// Force close connection to the client.
func (h *ServerConn) close() {
	h.conn.Close()
	h.log("stopped")
}

// Send packet back to the client side.
func (h *ServerConn) writePacket(packet proto.Packet) error {
	h.server.hit(SC_PACKET_WRITE)
	if err := h.conn.Send(packet); err != nil {
		h.server.hit(SC_PACKET_WRITE_ERRORS)
		h.log("packet write: %v", err)
		return err
	}
	return nil
}

// Process synchronous request from the client.
func (h *ServerConn) processRequest(req proto.Packet) {
	var (
		seqNum = req.SeqNum()
		reply  []byte
	)
	if f := h.server.config.RequestCallback; f != nil {
		reply = f(req.Data)
	}
	replyPacket := proto.NewReply(seqNum, reply)
	if err := h.writePacket(replyPacket); err == nil {
		h.log("sent reply: %v", replyPacket)
	} else {
		h.log("sent reply failed: %v", err)
	}
}

// Safely increment workers count.
func (h *ServerConn) incrementWorkers() {
	h.lock.Lock()
	h.workers++
	h.lock.Unlock()
	h.server.hit(SC_WORKER_ADDED)
}

// Safely decrement workers count.
func (h *ServerConn) decrementWorkers() {
	h.lock.Lock()
	h.workers--
	h.lock.Unlock()
	h.server.hit(SC_WORKER_REMOVED)
}

// Print message to the stdout if verbose mode is enabled.
func (h *ServerConn) log(format string, args ...interface{}) {
	if h.server.config.Trace {
		prefix := fmt.Sprintf("tcpcall srv%s %s> ", h.server.bindAddr, h.peer)
		log.Printf(prefix+format, args...)
	}
}
