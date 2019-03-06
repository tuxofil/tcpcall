package tcpcall

import (
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
)

type NodesCfg struct {
	nodes []string
	mu    sync.Mutex
}

func (n *NodesCfg) Nodes() []string {
	n.mu.Lock()
	res := n.nodes
	n.mu.Unlock()
	if res == nil {
		return []string{}
	}
	return res
}

func (n *NodesCfg) SetNodes(nodes ...string) {
	log.Printf("set peers to %v", nodes)
	n.mu.Lock()
	n.nodes = nodes
	n.mu.Unlock()
}

func (n *NodesCfg) Equal(nodes ...string) bool {
	return reflect.DeepEqual(n.Nodes(), nodes)
}

func TestPoolApplyPeers(t *testing.T) {
	cfg := NodesCfg{}
	pool_conf := NewPoolConf()
	pool_conf.ReconfigPeriod = time.Millisecond
	pool_conf.PeersFetcher = cfg.Nodes

	log.Printf("creating pool")
	pool := NewPool(pool_conf)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5010")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5000", "127.0.0.1:5010")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5000", "127.0.0.1:5010", "127.0.0.1:5020")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5000", "127.0.0.1:5020")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5020")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes()
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes("127.0.0.1:5000", "127.0.0.1:5010", "127.0.0.1:5020")
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}

	cfg.SetNodes()
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !cfg.Equal(pool.GetWorkerPeers()...) {
		t.Fatalf("%#v != %#v", cfg.Nodes(), pool.GetWorkerPeers())
	}
}

func TestPoolWorkersCount(t *testing.T) {
	cfg := NodesCfg{}
	poolConf := NewPoolConf()
	poolConf.ReconfigPeriod = time.Millisecond
	poolConf.ReconnectPeriod = time.Millisecond
	poolConf.PeersFetcher = cfg.Nodes
	pool := NewPool(poolConf)

	if pool.GetWorkersCount() != 0 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 0 {
		t.Fatal()
	}

	cfg.SetNodes("127.0.0.1:5024", "127.0.0.1:5025")
	time.Sleep(poolConf.ReconfigPeriod * 2)

	if pool.GetWorkersCount() != 2 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 0 {
		t.Fatal()
	}

	s1Conf := NewServerConf()
	s1Conf.PortNumber = 5024
	s1, err := Listen(s1Conf)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(poolConf.ReconnectPeriod * 2)

	if pool.GetWorkersCount() != 2 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 1 {
		t.Fatal()
	}

	s2Conf := NewServerConf()
	s2Conf.PortNumber = 5025
	s2, err := Listen(s2Conf)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(poolConf.ReconnectPeriod * 2)

	if pool.GetWorkersCount() != 2 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 2 {
		t.Fatal()
	}

	s1.Stop()
	time.Sleep(time.Millisecond * 300)

	if pool.GetWorkersCount() != 2 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 1 {
		t.Fatal()
	}

	s2.Stop()
	time.Sleep(time.Millisecond * 300)

	if pool.GetWorkersCount() != 2 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 0 {
		t.Fatal()
	}
}
