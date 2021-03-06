/*
Client connection pool.

Balance requests between client connections. Failover to next live
node on network failures or when one connection is overloaded.
Allow reconfiguration on-the-fly.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Connection pool counter indices.
// See Pool.counters field and Pool.Counters()
// method description for details.
const (
	// how many connection handlers were created
	PC_WORKER_ADDED = iota
	// how many connection handlers were removed
	PC_WORKER_REMOVED
	// one of connection handlers connects to the server
	PC_WORKER_CONNECT
	// one of connection handlers lost connection to the server
	PC_WORKER_DISCONNECT
	// reconfiguration loop ends
	PC_RECONFIG
	// connect/disconnect event received from one of
	// connection handlers
	PC_STATE_EVENT
	// suspend signal received from one of connection handlers
	PC_SUSPEND_EVENT
	// resume signal received from one of connection handlers
	PC_RESUME_EVENT
	// Req() method called
	PC_REQUESTS
	// Request to the server failed
	PC_REQUEST_ERRORS
	// Request retry occured
	PC_REQUEST_RETRIES
	// Cast() method called
	PC_CASTS
	// Cast to the server failed.
	PC_CAST_ERRORS
	// Cast retry occured.
	PC_CAST_RETRIES
	PC_COUNT // special value - count of all counters
)

// Connection pool state.
type Pool struct {
	// Configuration used to create Pool instance
	config PoolConf
	// List of all clients to all configured servers
	clients []*Client
	// List of clients known to be connected
	active []*Client
	// Pointer used for Round-Robin balancing
	balancerPointer int
	// Controls access to client lists above
	lock sync.RWMutex
	// Set to truth when pool is about terminating
	stopFlag bool
	// Channel used to receive state change events from
	// the clients (like connected/disconnected)
	stateEvents chan StateEvent
	// Channel used to receive suspend signals
	// from the clients.
	suspendEvents chan SuspendEvent
	// Channel used to receive resume signals
	// from the clients.
	resumeEvents chan ResumeEvent
	// Counters array
	counters   []int
	countersMu sync.RWMutex
}

// Connection pool configuration.
type PoolConf struct {
	// Static peer list to connect to.
	Peers []string
	// If not nil, result of the function will take
	// precedence of Peers value.
	// Set it to allow auto reconfiguration.
	PeersFetcher func() []string
	// Sleep duration between reconfiguration attempts.
	ReconfigPeriod time.Duration
	// Channel to send Uplink Cast data.
	UplinkCastListener chan UplinkCastEvent
	// Maximum parallel requests for one connection.
	Concurrency int
	// Sleep duration before reconnect after connection failure.
	ReconnectPeriod time.Duration
	// Request send max retry count. Negative value means count of
	// currently connected servers, 0 means no retries will
	// performed at all.
	MaxRequestRetries int
	// Asynchronous request send max retry count. Negative value
	// means count of currently connected servers, 0 means no
	// retries will performed at all.
	MaxCastRetries int
	// Max reply packet size, in bytes. 0 means no limit.
	MaxReplySize int
	// Minimum flush period for socket writer
	MinFlushPeriod time.Duration
	// Socket write buffer size
	WriteBufferSize int
	// Enable debug logging or not.
	Trace bool
	// Enable clients debug logging or not.
	ClientTrace bool
	// Optional interface to allocate byte slices for
	// input requests and casts. Allocator must return a slice of
	// exact size given as the argument.
	Allocator func(int) []byte
}

// Pool instance information returned by Info() method.
// Added mostly for debugging.
type PoolInfo struct {
	// Configuration used to create Pool instance
	Config PoolConf
	// Count of all clients to all configured servers
	ClientsCount int
	// Count of clients known to be connected
	ActiveCount int
	// Info and stats of all clients
	ClientStats []ClientInfo
	// Pointer used for Round-Robin balancing
	BalancerPointer int
	// Set to truth when pool is about terminating
	StopFlag bool
	// Count of unhandled client state change signals
	StateEventLen int
	// Count of unhandled client suspend signals
	SuspendEventLen int
	// Count of unhandled client resume signals
	ResumeEventLen int
	// Counters array
	Counters []int
}

// Create new connection pool.
func NewPool(conf PoolConf) *Pool {
	p := Pool{
		config:        conf,
		clients:       make([]*Client, 0),
		active:        make([]*Client, 0),
		stateEvents:   make(chan StateEvent, 10),
		suspendEvents: make(chan SuspendEvent, 10),
		resumeEvents:  make(chan ResumeEvent, 10),
		counters:      make([]int, PC_COUNT),
	}
	go startEventListenerDaemon(&p)
	go startConfiguratorDaemon(&p)
	return &p
}

// Create default pool configuration.
func NewPoolConf() PoolConf {
	return PoolConf{
		Peers:             []string{},
		PeersFetcher:      nil,
		ReconfigPeriod:    time.Second * 5,
		Concurrency:       gConcurrency,
		ReconnectPeriod:   time.Millisecond * 100,
		MaxRequestRetries: -1,
		MaxCastRetries:    -1,
		MinFlushPeriod:    gMinFlushPeriod,
		WriteBufferSize:   gWriteBufferSize,
		Trace:             gTracePool,
	}
}

// Make request.
func (p *Pool) ReqChunks(chunks [][]byte, timeout time.Duration) (rep []byte, err error) {
	return p.Req(bytes.Join(chunks, nil), timeout)
}

// Make request.
func (p *Pool) Req(data []byte, timeout time.Duration) (rep []byte, err error) {
	p.hit(PC_REQUESTS)
	deadline := time.Now().Add(timeout)
	retries := p.config.MaxRequestRetries
	if retries < 0 {
		retries = p.GetActiveWorkersCount()
	}
	var (
		lastError error
		retry     bool
	)
	for time.Now().Before(deadline) {
		if retry {
			p.hit(PC_REQUEST_RETRIES)
		}
		client := p.getNextActive()
		if client == nil {
			p.hit(PC_REQUEST_ERRORS)
			return nil, NotConnectedError
		}
		rep, err = client.Req(data, timeout)
		if err == nil {
			return rep, nil
		}
		p.hit(PC_REQUEST_ERRORS)
		lastError = err
		if canFailover(err) {
			if 0 < retries {
				// try next connected server
				retries--
				retry = true
				continue
			} else {
				break
			}
		}
		return nil, err
	}
	if !time.Now().Before(deadline) {
		return nil, TimeoutError
	}
	return nil, lastError
}

// Make asynchronous request to the server.
func (p *Pool) CastChunks(chunks [][]byte) error {
	return p.Cast(bytes.Join(chunks, nil))
}

// Make asynchronous request to the server.
func (p *Pool) Cast(data []byte) error {
	p.hit(PC_CASTS)
	retries := p.config.MaxCastRetries
	if retries < 0 {
		retries = p.GetActiveWorkersCount()
	}
	var retry bool
	for {
		if retry {
			p.hit(PC_CAST_RETRIES)
		}
		client := p.getNextActive()
		if client == nil {
			p.hit(PC_CAST_ERRORS)
			return NotConnectedError
		}
		err := client.Cast(data)
		if err == nil {
			return nil
		}
		p.hit(PC_CAST_ERRORS)
		if canFailover(err) {
			if 0 < retries {
				// try next connected server
				retries--
				retry = true
				continue
			}
		}
		return err
	}
}

// Return true if request can be retransmitted to another server.
func canFailover(err error) bool {
	switch err {
	case NotConnectedError:
		return true
	case DisconnectedError:
		// failed to send packet
		return true
	case OverloadError:
		return true
	}
	return false
}

// Select next worker from the list of connected workers.
func (p *Pool) getNextActive() (client *Client) {
	p.lock.Lock()
	if len(p.active) <= p.balancerPointer {
		p.balancerPointer = 0
	}
	if p.balancerPointer < len(p.active) {
		client = p.active[p.balancerPointer]
	}
	p.balancerPointer++
	p.lock.Unlock()
	return client
}

// Destroy the pool.
func (p *Pool) Close() {
	p.lock.Lock()
	if p.clients == nil || len(p.clients) == 0 {
		for _, c := range p.clients {
			c.Close()
		}
	}
	p.stopFlag = true
	p.lock.Unlock()
}

// Return address list of all connections in the pool.
func (p *Pool) GetWorkerPeers() []string {
	p.lock.RLock()
	res := make([]string, len(p.clients))
	for i := 0; i < len(p.clients); i++ {
		res[i] = p.clients[i].peer
	}
	p.lock.RUnlock()
	return res
}

// Return count of all workers.
func (p *Pool) GetWorkersCount() int {
	p.lock.RLock()
	res := len(p.clients)
	p.lock.RUnlock()
	return res
}

// Return count of active workers.
func (p *Pool) GetActiveWorkersCount() int {
	p.lock.RLock()
	res := len(p.active)
	p.lock.RUnlock()
	return res
}

// Return count of requests being processed by all workers.
func (p *Pool) GetQueuedRequests() (count int) {
	p.lock.RLock()
	for _, w := range p.clients {
		count += w.GetQueuedRequests()
	}
	p.lock.RUnlock()
	return count
}

// Return count of requests being processed by active workers.
func (p *Pool) GetActiveQueuedRequests() (count int) {
	p.lock.RLock()
	for _, w := range p.active {
		count += w.GetQueuedRequests()
	}
	p.lock.RUnlock()
	return count
}

// Get peers list from configuration.
func (p *Pool) getPeers() []string {
	if p.config.PeersFetcher != nil {
		return p.config.PeersFetcher()
	}
	return p.config.Peers
}

// Goroutine.
// Process all events received from connection handlers.
func startEventListenerDaemon(p *Pool) {
	p.log("daemon started")
	ticker := time.NewTicker(time.Millisecond * 200)
	for !p.Stopped() {
		select {
		case state_event := <-p.stateEvents:
			p.counters[PC_STATE_EVENT]++
			switch {
			case state_event.Online:
				p.publishWorker(state_event.Sender)
			case !state_event.Online:
				p.unpublishWorker(state_event.Sender)
			}
		case suspend := <-p.suspendEvents:
			p.counters[PC_SUSPEND_EVENT]++
			if p.unpublishWorker(suspend.Sender) {
				go func(d time.Duration) {
					time.Sleep(d)
					p.resumeEvents <- ResumeEvent{suspend.Sender}
				}(suspend.Duration)
			}
		case resume := <-p.resumeEvents:
			p.counters[PC_RESUME_EVENT]++
			p.publishWorker(resume.Sender)
		case <-ticker.C:
		}
	}
	ticker.Stop()
	p.log("daemon terminated")
}

// Goroutine.
// Reconfigures the pool on the fly.
func startConfiguratorDaemon(p *Pool) {
	p.log("reconfigurator daemon started")
	for !p.Stopped() {
		p.applyPeers(p.getPeers())
		p.counters[PC_RECONFIG]++
		time.Sleep(p.config.ReconfigPeriod)
	}
	p.log("reconfigurator daemon terminated")
}

// Return true when pool is closed and cannot be used anymore.
func (p *Pool) Stopped() bool {
	p.lock.RLock()
	res := p.stopFlag
	p.lock.RUnlock()
	return res
}

// Apply new list of target peers.
// Make incremental update of client connections list.
func (p *Pool) applyPeers(peers []string) {
	sort.Strings(peers)
	mlen := func() int {
		l := len(peers)
		if l < len(p.clients) {
			l = len(p.clients)
		}
		return l
	}
	for i := 0; i < mlen(); {
		if p.Stopped() {
			return
		}
		if i < len(peers) && i < len(p.clients) {
			switch {
			case peers[i] == p.clients[i].peer:
				i++
			case p.clients[i].peer < peers[i]:
				p.remWorker(i)
			case peers[i] < p.clients[i].peer:
				p.addWorker(i, peers[i])
				i++
			}
		} else if len(peers) <= i && i < len(p.clients) {
			p.remWorker(i)
		} else if i < len(peers) && len(p.clients) <= i {
			p.addWorker(i, peers[i])
			i++
		}
	}
}

// Remove client connection from the pool.
func (p *Pool) remWorker(index int) {
	p.lock.Lock()
	worker := p.clients[index]
	p.log("removing worker for %s", worker.peer)
	remFromArray(index, &p.clients)
	p.unpublishWorkerUnsafe(worker)
	worker.Close()
	p.lock.Unlock()
	p.counters[PC_WORKER_REMOVED]++
}

// Add new client connection to the pool.
func (p *Pool) addWorker(index int, peer string) {
	cfg := NewClientConf()
	cfg.Concurrency = p.config.Concurrency
	cfg.ReconnectPeriod = p.config.ReconnectPeriod
	cfg.MaxReplySize = p.config.MaxReplySize
	cfg.MinFlushPeriod = p.config.MinFlushPeriod
	cfg.WriteBufferSize = p.config.WriteBufferSize
	cfg.StateListener = p.stateEvents
	cfg.SuspendListener = p.suspendEvents
	cfg.ResumeListener = p.resumeEvents
	cfg.UplinkCastListener = p.config.UplinkCastListener
	cfg.SyncConnect = false
	cfg.Trace = p.config.ClientTrace
	cfg.Allocator = p.config.Allocator
	worker, _ := Dial(peer, cfg)
	p.log("adding worker for %s", peer)
	p.lock.Lock()
	addToArray(index, &p.clients, worker)
	p.lock.Unlock()
	p.counters[PC_WORKER_ADDED]++
}

// Add client connection to the list of active (connected) workers.
func (p *Pool) publishWorker(c *Client) {
	p.lock.Lock()
	for i := 0; i < len(p.active); i++ {
		if p.active[i] == c {
			// already in
			p.lock.Unlock()
			return
		}
	}
	p.log("publishing %s", c.peer)
	addToArray(0, &p.active, c)
	p.lock.Unlock()
	p.counters[PC_WORKER_CONNECT]++
}

// Remove client connection from the list of active (connected) workers.
// Return 'true' if the worker was really unpublished.
func (p *Pool) unpublishWorker(c *Client) bool {
	p.lock.Lock()
	res := p.unpublishWorkerUnsafe(c)
	p.lock.Unlock()
	return res
}

// Remove client connection from the list of active (connected) workers.
// Return 'true' if the worker was really unpublished.
func (p *Pool) unpublishWorkerUnsafe(c *Client) bool {
	for i := 0; i < len(p.active); i++ {
		if p.active[i] == c {
			p.log("unpublishing %s", c.peer)
			remFromArray(i, &p.active)
			p.counters[PC_WORKER_DISCONNECT]++
			return true
		}
	}
	return false
}

// Remove element from array of clients
func remFromArray(index int, a *[]*Client) {
	b := make([]*Client, len(*a)-1)
	copy(b, (*a)[0:index])
	copy(b[index:], (*a)[index+1:])
	*a = b
}

// Insert new client to the array.
func addToArray(index int, a *[]*Client, c *Client) {
	b := make([]*Client, len(*a)+1)
	copy(b, (*a)[0:index])
	copy(b[index+1:], (*a)[index:])
	b[index] = c
	*a = b
}

// Print message to the stdout if verbose mode is enabled.
func (p *Pool) log(format string, args ...interface{}) {
	if p.config.Trace {
		prefix := fmt.Sprintf("tcpcall pool %v> ", p.getPeers())
		log.Printf(prefix+format, args...)
	}
}

// Thread safe increment of internal counter.
func (p *Pool) hit(counter int) {
	p.countersMu.Lock()
	p.counters[counter]++
	p.countersMu.Unlock()
}

// Return a snapshot of all internal counters.
func (p *Pool) Counters() []int {
	res := make([]int, PC_COUNT)
	p.countersMu.RLock()
	copy(res, p.counters)
	p.countersMu.RUnlock()
	return res
}

// Return pool's info and stats.
func (p *Pool) Info() PoolInfo {
	info := PoolInfo{
		Config:          p.config,
		ClientStats:     []ClientInfo{},
		BalancerPointer: p.balancerPointer,
		StopFlag:        p.stopFlag,
		StateEventLen:   len(p.stateEvents),
		SuspendEventLen: len(p.suspendEvents),
		ResumeEventLen:  len(p.resumeEvents),
		Counters:        p.Counters(),
	}
	p.lock.RLock()
	for _, c := range p.clients {
		cinfo := c.Info()
		info.ClientsCount++
		if cinfo.Connected {
			info.ActiveCount++
		}
		info.ClientStats = append(info.ClientStats, cinfo)
	}
	p.lock.RUnlock()
	return info
}
