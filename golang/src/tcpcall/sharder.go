/**
Sharder allows to balance requests among configured nodes
according to some request ID. Each request with the same ID
will be routed to the same node.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 13 Oct 2017
Copyright: 2017, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Sharder counter indices.
// See Sharder.counters field and Sharder.Counters()
// method description for details.
const (
	// How many requests were made
	SHC_REQUESTS = iota
	// How many successfull reconfigs were made
	SHC_RECONFIGS
	// How many times reconfiguration failed
	SHC_RECONFIG_FAILS
	// How many connection pools were created
	SHC_POOLS_CREATED
	// How many connection pools were closed
	SHC_POOLS_CLOSED
	SHC_COUNT // special value - count of all counters
)

type Sharder struct {
	// Configuration used to create Sharder instance
	config SharderConf
	// List of servers actually used
	nodes []string
	// Map server hostname to connection pool
	conns map[string]*Pool
	// Controls access to the map of connection pools
	mu *sync.RWMutex
	// Counters array
	counters   []int
	countersMu sync.Locker
}

// Sharder configuration
type SharderConf struct {
	// Callback function used to get list of servers
	NodesGetter func() ([]string, error)
	// How often NodesGetter will be called
	ReconfigPeriod time.Duration
	// How many connections will be established to each server
	ConnsPerNode int
	// Request send max retry count. Negative value means count of
	// currently established connections to one server,
	// 0 means no retries will performed at all.
	MaxRequestRetries int
}

// Sharder instance information returned by Info() method.
// Added mostly for debugging.
type SharderInfo struct {
	// Configuration used to create Sharder instance
	Config SharderConf
	// List of nodes actually used
	Nodes []string
	// Pools info and stats
	Pools []PoolInfo
	// Counters array
	Counters []int
}

// Create new Sharder instance with given configuration.
func NewSharder(config SharderConf) *Sharder {
	sharder := &Sharder{
		config:     config,
		nodes:      []string{},
		conns:      map[string]*Pool{},
		mu:         &sync.RWMutex{},
		counters:   make([]int, SHC_COUNT),
		countersMu: &sync.Mutex{},
	}
	go sharder.reconfigLoop()
	return sharder
}

// Create default configuration for Sharder.
func NewSharderConf() SharderConf {
	return SharderConf{
		NodesGetter: func() ([]string, error) {
			return []string{}, nil
		},
		ReconfigPeriod:    10 * time.Second,
		ConnsPerNode:      5,
		MaxRequestRetries: -1,
	}
}

// Send request to one of configured servers. ID will not
// be sent, but used only to balance request between servers.
func (s *Sharder) Req(id, body []byte, timeout time.Duration) (reply []byte, err error) {
	s.hit(SHC_REQUESTS)
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodesCount := len(s.nodes)
	if nodesCount == 0 {
		return nil, errors.New("not configured")
	}
	i := shard(id, nodesCount)
	return s.conns[s.nodes[i]].Req(body, timeout)
}

// Return a snapshot of all internal counters.
func (s *Sharder) Counters() []int {
	res := make([]int, SHC_COUNT)
	s.countersMu.Lock()
	copy(res, s.counters)
	s.countersMu.Unlock()
	return res
}

// Return Sharder's info and stats.
func (s *Sharder) Info() SharderInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info := SharderInfo{
		Config: s.config,
		Nodes:  s.nodes,
		Pools:  []PoolInfo{},
	}
	for _, v := range s.conns {
		info.Pools = append(info.Pools, v.Info())
	}
	return info
}

// Sharding function.
func shard(id []byte, n int) int {
	hash := md5.Sum(id)
	return int(binary.BigEndian.Uint64(hash[:8]) % uint64(n))
}

// Goroutine.
// Reconfigure clients pool on the fly.
func (s *Sharder) reconfigLoop() {
	for {
		if nodes, err := s.config.NodesGetter(); err == nil {
			s.setNodes(nodes)
			s.counters[SHC_RECONFIGS]++
			time.Sleep(s.config.ReconfigPeriod)
		} else {
			s.counters[SHC_RECONFIG_FAILS]++
			time.Sleep(s.config.ReconfigPeriod / 10)
		}
	}
}

// Apply new node list to the clients pool.
func (s *Sharder) setNodes(newNodes []string) {
	newNodes = uniq(newNodes)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range newNodes {
		if _, ok := s.conns[n]; !ok {
			poolCfg := NewPoolConf()
			peers := make([]string, s.config.ConnsPerNode)
			for i := 0; i < s.config.ConnsPerNode; i++ {
				peers[i] = n
			}
			poolCfg.Peers = peers
			poolCfg.MaxRequestRetries = s.config.MaxRequestRetries
			s.conns[n] = NewPool(poolCfg)
			s.counters[SHC_POOLS_CREATED]++
		}
	}
	for k, p := range s.conns {
		if !inList(k, newNodes) {
			p.Close()
			delete(s.conns, k)
			s.counters[SHC_POOLS_CLOSED]++
		}
	}
	s.nodes = newNodes
}

// Thread safe counter increment.
func (s *Sharder) hit(counter int) {
	s.countersMu.Lock()
	s.counters[counter]++
	s.countersMu.Unlock()
}

// Leave only unique elements from a string array.
func uniq(a []string) []string {
	r := []string{}
	for _, v := range a {
		if !inList(v, r) {
			r = append(r, v)
		}
	}
	return r
}

// Return truth if array a contains string s at least once.
func inList(s string, a []string) bool {
	for _, e := range a {
		if s == e {
			return true
		}
	}
	return false
}
