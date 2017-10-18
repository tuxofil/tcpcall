package tcpcall

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

type Sharder struct {
	config SharderConfig
	nodes  []string
	conns  map[string]*Pool
	mu     *sync.RWMutex
}

type SharderConfig struct {
	NodesGetter    func() ([]string, error)
	ReconfigPeriod time.Duration
	ConnsPerNode   int
}

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

func NewSharderConfig() SharderConfig {
	return SharderConfig{
		NodesGetter: func() ([]string, error) {
			return []string{}, nil
		},
		ReconfigPeriod: 10 * time.Second,
		ConnsPerNode:   5,
	}
}

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

func shard(id []byte, n int) int {
	hash := md5.Sum(id)
	return int(binary.BigEndian.Uint64(hash[:8]) % uint64(n))
}

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

func (s *Sharder) setNodes(newNodes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	newNodes = uniq(newNodes)
	for _, n := range newNodes {
		if _, ok := s.conns[n]; !ok {
			poolCfg := NewPoolConf()
			peers := make([]string, s.config.ConnsPerNode)
			for i := 0; i < s.config.ConnsPerNode; i++ {
				peers[i] = n
			}
			poolCfg.Peers = peers
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

func uniq(a []string) []string {
	r := []string{}
	for _, v := range a {
		if !inList(v, r) {
			r = append(r, v)
		}
	}
	return r
}

func inList(s string, a []string) bool {
	for _, e := range a {
		if s == e {
			return true
		}
	}
	return false
}
