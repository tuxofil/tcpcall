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
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Connection pool state.
type Pool struct {
	config          PoolConf
	clients         []*Client
	active          []*Client
	balancerPointer int
	lock            sync.Locker
	stopFlag        bool
	stateEvents     chan StateEvent
	suspendEvents   chan SuspendEvent
	resumeEvents    chan ResumeEvent
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
}

// Create new connection pool.
func NewPool(conf PoolConf) *Pool {
	p := Pool{
		config:        conf,
		clients:       make([]*Client, 0),
		active:        make([]*Client, 0),
		lock:          &sync.Mutex{},
		stateEvents:   make(chan StateEvent, 10),
		suspendEvents: make(chan SuspendEvent, 10),
		resumeEvents:  make(chan ResumeEvent, 10),
	}
	go startEventListenerDaemon(&p)
	go startConfiguratorDaemon(&p)
	return &p
}

// Create default pool configuration.
func NewPoolConf() PoolConf {
	return PoolConf{
		Peers:           []string{},
		PeersFetcher:    nil,
		ReconfigPeriod:  time.Second * 5,
		Concurrency:     defConcurrency,
		ReconnectPeriod: time.Millisecond * 100,
		MinFlushPeriod:  defMinFlush,
		WriteBufferSize: defWBufSize,
		Trace:           tracePool,
	}
}

// Make request.
func (p *Pool) Req(bytes []byte, timeout time.Duration) (rep []byte, err error) {
	return p.ReqChunks([][]byte{bytes}, timeout)
}

// Make request.
func (p *Pool) ReqChunks(bytes [][]byte, timeout time.Duration) (rep []byte, err error) {
	deadline := time.Now().Add(timeout)
	retry_count := len(p.active)
	err = NotConnectedError
	for time.Now().Before(deadline) && 0 < retry_count {
		if c := p.getNextActive(); c != nil {
			rep, err = c.ReqChunks(bytes, timeout)
			if canFailover(err) {
				// try next connected server
				retry_count--
				continue
			}
			return rep, err
		}
		return nil, err
	}
	if !time.Now().Before(deadline) {
		return nil, TimeoutError
	}
	return nil, err
}

// Make asynchronous request to the server.
func (p *Pool) Cast(data []byte) error {
	return p.CastChunks([][]byte{data})
}

// Make asynchronous request to the server.
func (p *Pool) CastChunks(data [][]byte) error {
	active_count := len(p.active)
	if active_count == 0 {
		return NotConnectedError
	}
	var err error
	for i := 0; i < active_count; i++ {
		if c := p.getNextActive(); c != nil {
			err = c.CastChunks(data)
			if canFailover(err) {
				// try next connected server
				continue
			}
			return err
		}
	}
	return err // return last error
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
	defer p.lock.Unlock()
	if len(p.active) <= p.balancerPointer {
		p.balancerPointer = 0
	}
	if p.balancerPointer < len(p.active) {
		client = p.active[p.balancerPointer]
	}
	p.balancerPointer++
	return client
}

// Destroy the pool.
func (p *Pool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.clients == nil || len(p.clients) == 0 {
		for _, c := range p.clients {
			c.Close()
		}
	}
	p.stopFlag = true
}

// Return address list of all connections in the pool.
func (p *Pool) GetWorkerPeers() []string {
	p.lock.Lock()
	defer p.lock.Unlock()
	res := make([]string, len(p.clients))
	for i := 0; i < len(p.clients); i++ {
		res[i] = p.clients[i].peer
	}
	return res
}

// Return count of all workers.
func (p *Pool) GetWorkersCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.clients)
}

// Return count of active workers.
func (p *Pool) GetActiveWorkersCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.active)
}

// Return count of requests being processed by all workers.
func (p *Pool) GetQueuedRequests() (count int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, w := range p.clients {
		count += w.GetQueuedRequests()
	}
	return count
}

// Return count of requests being processed by active workers.
func (p *Pool) GetActiveQueuedRequests() (count int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, w := range p.active {
		count += w.GetQueuedRequests()
	}
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
	defer p.log("daemon terminated")
	for !p.stopFlag {
		select {
		case state_event := <-p.stateEvents:
			switch {
			case state_event.Online:
				p.publishWorker(state_event.Sender)
			case !state_event.Online:
				p.unpublishWorker(state_event.Sender)
			}
		case suspend := <-p.suspendEvents:
			if p.unpublishWorker(suspend.Sender) {
				go func() {
					time.Sleep(suspend.Duration)
					p.resumeEvents <- ResumeEvent{suspend.Sender}
				}()
			}
		case resume := <-p.resumeEvents:
			p.publishWorker(resume.Sender)
		case <-time.After(time.Millisecond * 200):
		}
	}
}

// Goroutine.
// Reconfigures the pool on the fly.
func startConfiguratorDaemon(p *Pool) {
	p.log("reconfigurator daemon started")
	defer p.log("reconfigurator daemon terminated")
	for !p.stopFlag {
		p.applyPeers(p.getPeers())
		time.Sleep(p.config.ReconfigPeriod)
	}
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
		if p.stopFlag {
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
	defer p.lock.Unlock()
	worker := p.clients[index]
	p.log("removing worker for %s", worker.peer)
	remFromArray(index, &p.clients)
	p.unpublishWorker(worker)
	worker.Close()
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
	worker, _ := Dial(peer, cfg)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.log("adding worker for %s", peer)
	addToArray(index, &p.clients, worker)
}

// Add client connection to the list of active (connected) workers.
func (p *Pool) publishWorker(c *Client) {
	for i := 0; i < len(p.active); i++ {
		if p.active[i] == c {
			// already in
			return
		}
	}
	p.log("publishing %s", c.peer)
	addToArray(0, &p.active, c)
}

// Remove client connection from the list of active (connected) workers.
// Return 'true' if the worker was really unpublished.
func (p *Pool) unpublishWorker(c *Client) bool {
	for i := 0; i < len(p.active); i++ {
		if p.active[i] == c {
			p.log("unpublishing %s", c.peer)
			remFromArray(i, &p.active)
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

// Compare two string arrays for equality.
func equals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Print message to the stdout if verbose mode is enabled.
func (p *Pool) log(format string, args ...interface{}) {
	if p.config.Trace {
		prefix := fmt.Sprintf("tcpcall pool %v> ", p.getPeers())
		log.Printf(prefix+format, args...)
	}
}
