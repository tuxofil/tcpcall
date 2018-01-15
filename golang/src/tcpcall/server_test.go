package tcpcall

import (
	"testing"
	"time"
)

func TestUplinkCast(t *testing.T) {
	// initialize
	serverCfg := NewServerConf()
	serverCfg.PortNumber = 5000
	server, err := Listen(serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	poolCfg := NewPoolConf()
	poolCfg.ReconfigPeriod = time.Millisecond
	poolCfg.UplinkCastListener = make(chan UplinkCastEvent)
	poolCfg.Peers = []string{"127.0.0.1:5000"}
	pool := NewPool(poolCfg)
	// warming up
	time.Sleep(poolCfg.ReconfigPeriod * 10)
	// do the test
	data := "foo"
	server.UplinkCast([]byte(data))
	select {
	case event := <-poolCfg.UplinkCastListener:
		found := string(event.Data)
		if found != data {
			t.Fatalf("expected %#v but %#v found", data, found)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeouted")
	}
	// clean up
	server.Stop()
	pool.Close()
}
