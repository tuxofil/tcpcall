package tcpcall

import (
	"log"
	"testing"
	"time"
)

func TestEquals(t *testing.T) {
	data := []struct {
		a []string
		b []string
		bool
	}{
		{[]string{}, []string{}, true},
		{[]string{}, []string{"1"}, false},
		{[]string{"1"}, []string{"1"}, true},
		{[]string{"1"}, []string{"1", "2"}, false},
	}
	for i := 0; i < len(data); i++ {
		e := equals(data[i].a, data[i].b)
		if e != data[i].bool {
			t.Errorf(
				"%v and %v. Expected %v but %v found",
				data[i].a, data[i].b, data[i].bool, e)
		}
	}
}

func TestApplyPeers(t *testing.T) {
	cfg := []string{}
	pool_conf := NewPoolConf()
	pool_conf.ReconfigPeriod = time.Millisecond
	fetcher := func() []string {
		return cfg
	}
	pool_conf.PeersFetcher = &fetcher

	log.Printf("creating pool")
	pool := NewPool(pool_conf)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5010"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5000", "127.1:5010"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5000", "127.1:5010", "127.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5000", "127.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.1:5000", "127.1:5010", "127.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}
}
