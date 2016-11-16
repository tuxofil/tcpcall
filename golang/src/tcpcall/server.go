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
	"encoding/binary"
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
	stop_flag   bool
}

// Server configuration
type ServerConf struct {
	// Maximum simultaneous connections to accept
	MaxConnections int
	// Maximum request processing concurrency per connection.
	Concurrency int
	// Max request packet size, in bytes. 0 means no limit.
	MaxRequestSize int
	// Request processing function.
	// Each request will be processed in parallel with others.
	RequestCallback *func([]byte) []byte
	// Asynchronous request (cast) processing function.
	// Each request will be processed in parallel with others.
	CastCallback *func([]byte)
	// Duration for suspend signals
	SuspendDuration time.Duration
	// Enable debug logging or not
	Trace bool
}

// Connection handler state
type ServerConn struct {
	peer      string
	conn      net.Conn
	workers   int
	lock      *sync.Mutex
	server    *Server
	stop_flag bool
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
		Trace:           trace_server,
	}
}

// Stop the server.
func (s *Server) Stop() {
	s.stop_flag = true
	s.lock.Lock()
	defer s.lock.Unlock()
	for h, _ := range s.connections {
		h.close()
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
	for !s.stop_flag {
		if s.config.MaxConnections <= len(s.connections) {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		socket, err := s.socket.Accept()
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		handler := ServerConn{
			peer:   socket.RemoteAddr().String(),
			conn:   socket,
			server: s,
			lock:   &sync.Mutex{},
		}
		s.lock.Lock()
		s.connections[&handler] = true
		s.lock.Unlock()
		go handler.handleTcpConnection()
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

// Goroutine.
// Handle TCP connection from client.
func (h *ServerConn) handleTcpConnection() {
	h.log("accepted")
	defer func() {
		h.server.lock.Lock()
		delete(h.server.connections, h)
		h.server.lock.Unlock()
		h.conn.Close()
		h.log("connection closed")
	}()
	for !h.stop_flag {
		if h.server.config.Concurrency < h.workers {
			// max workers count reached
			err := h.suspend(h.server.config.SuspendDuration)
			if err != nil {
				h.log("suspend send: %v", err)
				return
			}
			time.Sleep(time.Millisecond * 100)
			continue
		}
		packet_type, data, err := h.readNextPacket()
		if err != nil {
			return
		}
		h.log("got packet of type %d: %v", packet_type, data)
		h.process(packet_type, data)
	}
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
	h.stop_flag = true
	h.log("stopped")
}

// Send packet back to the client side.
func (h *ServerConn) writePacket(packet proto.Packet) error {
	bytes := packet.Encode()
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(bytes)))
	h.lock.Lock()
	defer h.lock.Unlock()
	_, err := h.conn.Write(header)
	if err != nil {
		h.log("header write: %v", err)
		return err
	}
	_, err = h.conn.Write(bytes)
	if err != nil {
		h.log("packet write: %v", err)
		return err
	}
	return nil
}

func (h *ServerConn) process(packet_type int, data interface{}) {
	switch packet_type {
	case proto.REQUEST:
		go h.processRequest(data.(*proto.PacketRequest))
	case proto.CAST:
		go func() {
			h.incrementWorkers()
			defer h.decrementWorkers()
			if h.server.config.CastCallback != nil {
				(*h.server.config.CastCallback)((data.(*proto.PacketCast)).Request)
			}
		}()
	default:
		// ignore packet
	}
}

// Process synchronous request from the client.
func (h *ServerConn) processRequest(req *proto.PacketRequest) {
	h.incrementWorkers()
	defer h.decrementWorkers()
	var res []byte
	if h.server.config.RequestCallback != nil {
		f := *h.server.config.RequestCallback
		res = f(req.Request)
	} else {
		res = []byte{}
	}
	reply_packet := proto.PacketReply{req.SeqNum, res}
	err := h.writePacket(reply_packet)
	if err == nil {
		h.log("sent reply: %v", reply_packet)
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

// Read and decode next packet from the network.
func (h *ServerConn) readNextPacket() (ptype int, packet interface{}, err error) {
	header := make([]byte, 4)
	// read packet header
	n, err := h.conn.Read(header)
	if err != nil {
		h.log("header read: %v", err)
		return -1, nil, err
	}
	if n != len(header) {
		h.log("header too small")
		return -1, nil, err
	}
	// read packet
	plen := int(binary.BigEndian.Uint32(header))
	if 0 < h.server.config.MaxRequestSize && h.server.config.MaxRequestSize < plen {
		return -1, nil, fmt.Errorf("request packet too long (%d bytes)", plen)
	}
	encoded_packet := make([]byte, plen)
	n, err = h.conn.Read(encoded_packet)
	if err != nil {
		h.log("packet read: %v", err)
		return -1, nil, err
	}
	if n != len(encoded_packet) {
		h.log("packet too small")
		return -1, nil, err
	}
	// decode packet
	ptype, packet, err = proto.Decode(encoded_packet)
	if err != nil {
		h.log("packet decode failed: %v", err)
		return -1, nil, err
	}
	return ptype, packet, err
}

// Print message to the stdout if verbose mode is enabled.
func (h *ServerConn) log(format string, args ...interface{}) {
	if h.server.config.Trace {
		prefix := fmt.Sprintf("tcpcall srv%s %s> ", h.server.bindAddr, h.peer)
		log.Printf(prefix+format, args...)
	}
}
