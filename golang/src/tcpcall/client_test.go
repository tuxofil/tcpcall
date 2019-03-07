package tcpcall

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestClientConcurrency(t *testing.T) {
	// configure
	concurrency := 500
	// create server
	serverConf := NewServerConf()
	serverConf.PortNumber = 6000
	serverConf.MaxConnections = 1
	serverConf.Concurrency = concurrency
	serverConf.RequestCallback = func(data []byte) []byte {
		return serve(t, data)
	}
	server, err := Listen(serverConf)
	if err != nil {
		t.Fatalf("listen: %s", err)
	}
	// create client
	clientConf := NewClientConf()
	clientConf.Concurrency = concurrency
	client, err := Dial("127.0.0.1:6000", clientConf)
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	// issue a lot of requests in parallel
	var (
		wg  sync.WaitGroup
		chn = make(chan int, concurrency)
	)
	log.Printf("spawning...")
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go workerLoop(t, &wg, chn, client)
	}
	log.Printf("communicating...")
	for i := 0; i < 500000; i++ {
		chn <- i
	}
	log.Printf("terminating...")
	close(chn)
	wg.Wait()
	log.Printf("cleanup...")
	client.Close()
	server.Stop()
}

func workerLoop(t *testing.T, wg *sync.WaitGroup, chn chan int, client *Client) {
	defer wg.Done()
	for {
		sig, ok := <-chn
		if !ok {
			return
		}
		req := genReq(sig)
		rep, err := client.Req(req, 2*time.Second)
		if err != nil {
			t.Fatalf("req #%d failed: %s", sig, err)
		}
		if !bytes.Equal(req, rep) {
			t.Fatalf("req #%d: assert failed: %v != %v", sig, req, rep)
		}
	}
}

func serve(t *testing.T, data []byte) []byte {
	if err := decodeReq(data); err != nil {
		t.Errorf("server: failed to decode %v: %v", data, err)
	}
	return data
}

func genReq(id int) []byte {
	size := rand.Intn(256)
	msg := make([]byte, 5+size)
	binary.BigEndian.PutUint32(msg, uint32(id))
	msg[4] = byte(size)
	for i := 0; i < size; i++ {
		msg[5+i] = byte(size + id)
	}
	return msg
}

func decodeReq(data []byte) error {
	if len(data) < 5 {
		return errors.New("too short header")
	}
	id := int(binary.BigEndian.Uint32(data))
	size := int(data[4])
	if len(data) != 5+size {
		return fmt.Errorf("size mismatch. Expected %d but found %d",
			5+size, len(data))
	}
	expect := byte(size + id)
	for i := 0; i < size; i++ {
		if data[5+i] != expect {
			return fmt.Errorf("byte #%d. Expected %d but found %d",
				5+i, expect, data[5+i])
		}
	}
	return nil
}
