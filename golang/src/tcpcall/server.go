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

// Server state
type Server struct {
	bindAddr    string
	config      ServerConf
	socket      net.Listener
	connections map[*ServerConn]bool
	lock        *sync.Mutex
	stopFlag    bool
}

// Server configuration
type ServerConf struct {
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
	// Duration for suspend signals
	SuspendDuration time.Duration
	// Enable debug logging or not
	Trace bool
}

// Connection handler state
type ServerConn struct {
	peer    string
	conn    *MsgConn
	workers int
	lock    *sync.Mutex
	server  *Server
}

// Start new server.
func Listen(port uint16, conf ServerConf) (*Server, error) {
	bindAddr := fmt.Sprintf(":%d", port)
	socket, err := net.Listen("tcp", bindAddr)
	if err == nil {
		server := &Server{
			bindAddr:    bindAddr,
			config:      conf,
			socket:      socket,
			connections: make(map[*ServerConn]bool, conf.MaxConnections),
			lock:        &sync.Mutex{},
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
		MinFlushPeriod:  defMinFlush,
		WriteBufferSize: defWBufSize,
		Trace:           traceServer,
	}
}

// Stop the server.
func (s *Server) Stop() {
	s.stopFlag = true
	s.lock.Lock()
	defer s.lock.Unlock()
	for h, _ := range s.connections {
		go h.close()
	}
}

// Send 'suspend' signal to all connected clients.
func (s *Server) Suspend(duration time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for h, _ := range s.connections {
		h.suspend(duration)
	}
}

// Send 'resume' signal to all connected clients.
func (s *Server) Resume() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for h, _ := range s.connections {
		h.resume()
	}
}

// Goroutine.
// Accept incoming connections.
func (s *Server) acceptLoop() {
	defer s.socket.Close()
	s.log("daemon started")
	defer s.log("daemon terminated")
	for !s.stopFlag {
		if s.config.MaxConnections <= len(s.connections) {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		socket, err := s.socket.Accept()
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		if s.stopFlag {
			socket.Close()
			return
		}
		h := &ServerConn{
			peer:   socket.RemoteAddr().String(),
			server: s,
			lock:   &sync.Mutex{},
		}
		msgConn, err := NewMsgConn(socket,
			s.config.MinFlushPeriod,
			s.config.WriteBufferSize,
			h.onRecv, h.onClose)
		if err != nil {
			socket.Close()
			continue
		}
		h.conn = msgConn
		h.log("accepted")
		s.lock.Lock()
		s.connections[h] = true
		s.lock.Unlock()
	}
}

// Return count of active client connections opened.
func (s *Server) GetConnections() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.connections)
}

// Return total count of running requests.
func (s *Server) GetWorkers() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	workers := 0
	for h, _ := range s.connections {
		workers += h.workers
	}
	return workers
}

// Print message to the stdout if verbose mode is enabled.
func (s *Server) log(format string, args ...interface{}) {
	if s.config.Trace {
		prefix := fmt.Sprintf("tcpcall srv%s> ", s.bindAddr)
		log.Printf(prefix+format, args...)
	}
}

// Callback for message-oriented socket.
// Handles connection close event.
func (h *ServerConn) onClose() {
	h.server.lock.Lock()
	delete(h.server.connections, h)
	h.server.lock.Unlock()
	h.log("connection closed")
}

// Callback for message-oriented socket.
// Handles incoming message from the remote side.
func (h *ServerConn) onRecv(packet []byte) {
	if h.server.config.Concurrency < h.workers {
		// max workers count reached
		if err := h.suspend(h.server.config.SuspendDuration); err != nil {
			h.log("suspend send: %v", err)
			h.close()
		}
		return
	}
	h.incrementWorkers()
	go func() {
		defer h.decrementWorkers()
		// decode packet
		ptype, data, err := proto.Decode(packet)
		if err != nil {
			h.log("packet decode failed: %v", err)
			h.close()
			return
		}
		h.log("got packet of type %d: %v", ptype, data)
		switch ptype {
		case proto.REQUEST:
			h.processRequest(data.(*proto.PacketRequest))
		case proto.CAST:
			if f := h.server.config.CastCallback; f != nil {
				f(flatten((data.(*proto.PacketCast)).Request))
			}
		default:
			// ignore packet
		}
	}()
}

// Send 'suspend' signal to the connected client.
func (h *ServerConn) suspend(duration time.Duration) error {
	packet := proto.PacketFlowControlSuspend{duration}
	return h.writePacket(packet)
}

// Send 'resume' signal to the connected client.
func (h *ServerConn) resume() error {
	packet := proto.PacketFlowControlResume{}
	return h.writePacket(packet)
}

// Force close connection to the client.
func (h *ServerConn) close() {
	h.conn.Close()
	h.log("stopped")
}

// Send packet back to the client side.
func (h *ServerConn) writePacket(packet proto.Packet) error {
	if err := h.conn.Send(packet.Encode()); err != nil {
		h.log("packet write: %v", err)
		return err
	}
	return nil
}

// Process synchronous request from the client.
func (h *ServerConn) processRequest(req *proto.PacketRequest) {
	var res []byte
	if f := h.server.config.RequestCallback; f != nil {
		res = f(flatten(req.Request))
	} else {
		res = []byte{}
	}
	replyPacket := proto.PacketReply{req.SeqNum, [][]byte{res}}
	err := h.writePacket(replyPacket)
	if err == nil {
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
}

// Safely decrement workers count.
func (h *ServerConn) decrementWorkers() {
	h.lock.Lock()
	h.workers--
	h.lock.Unlock()
}

// Print message to the stdout if verbose mode is enabled.
func (h *ServerConn) log(format string, args ...interface{}) {
	if h.server.config.Trace {
		prefix := fmt.Sprintf("tcpcall srv%s %s> ", h.server.bindAddr, h.peer)
		log.Printf(prefix+format, args...)
	}
}
