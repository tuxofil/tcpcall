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

	cfg = []string{"127.0.0.1:5010"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.0.0.1:5000", "127.0.0.1:5010"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.0.0.1:5000", "127.0.0.1:5010", "127.0.0.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.0.0.1:5000", "127.0.0.1:5020"}
	log.Printf("set peers to %v", cfg)
	time.Sleep(pool_conf.ReconfigPeriod * 2)
	if !equals(cfg, pool.GetWorkerPeers()) {
		t.Fatal()
	}

	cfg = []string{"127.0.0.1:5020"}
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

	cfg = []string{"127.0.0.1:5000", "127.0.0.1:5010", "127.0.0.1:5020"}
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

func TestWorkersCount(t *testing.T) {
	cfg := []string{}
	poolConf := NewPoolConf()
	poolConf.ReconfigPeriod = time.Millisecond
	poolConf.ReconnectPeriod = time.Millisecond
	fetcher := func() []string {
		return cfg
	}
	poolConf.PeersFetcher = &fetcher
	pool := NewPool(poolConf)

	if pool.GetWorkersCount() != 0 {
		t.Fatal()
	}
	if pool.GetActiveWorkersCount() != 0 {
		t.Fatal()
	}

	cfg = []string{"127.0.0.1:5024", "127.0.0.1:5025"}
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
