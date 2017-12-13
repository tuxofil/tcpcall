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

type Sharder struct {
	// Configuration used to create Sharder instance
	config SharderConfig
	// List of servers actually used
	nodes []string
	// Map server hostname to connection pool
	conns map[string]*Pool
	// Controls access to the map of connection pools
	mu *sync.RWMutex
}

// Sharder configuration
type SharderConfig struct {
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

// Create new Sharder instance with given configuration.
func NewSharder(config SharderConfig) *Sharder {
	sharder := &Sharder{
		config: config,
		nodes:  []string{},
		conns:  map[string]*Pool{},
		mu:     &sync.RWMutex{},
	}
	go sharder.reconfigLoop()
	return sharder
}

// Create default configuration for Sharder.
func NewSharderConfig() SharderConfig {
	return SharderConfig{
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodesCount := len(s.nodes)
	if nodesCount == 0 {
		return nil, errors.New("not configured")
	}
	i := shard(id, nodesCount)
	return s.conns[s.nodes[i]].Req(body, timeout)
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
			time.Sleep(s.config.ReconfigPeriod)
		} else {
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
		}
	}
	for k, p := range s.conns {
		if !inList(k, newNodes) {
			p.Close()
			delete(s.conns, k)
		}
	}
	s.nodes = newNodes
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
