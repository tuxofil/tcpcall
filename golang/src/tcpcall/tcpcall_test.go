package tcpcall

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

const trace_enabled = false

/*
 ----------------------------------------------------------------------
  Client-Server Tests
 ----------------------------------------------------------------------
*/

func TestClientReqRep(t *testing.T) {
	s := startServer(t, 5001)
	defer s.Stop()
	c, _ := Dial(":5001", client_conf)
	defer c.Close()
	// make some requests
	MkReq(t, c, 1, time.Millisecond*50, time.Millisecond*100, nil)
	MkReq(t, c, 2, time.Millisecond*50, time.Millisecond*20, TimeoutError)
	s.Stop()
	MkReq(t, c, 3, time.Millisecond*50, time.Millisecond*100, ANY)
	sleep(time.Second)
	s = startServer(t, 5001)
	sleep(time.Millisecond * 200)
	MkReq(t, c, 4, time.Millisecond*50, time.Millisecond*100, nil)
}

func TestClientCast(t *testing.T) {
	s := startServers(t, 5002)
	defer stopServers(s)
	c, _ := Dial(":5002", client_conf)
	defer c.Close()
	MkCast(t, c, 0, nil)
	MkCast(t, c, 1, nil)
	MkCast(t, c, 2, nil)
}

func TestClientParallelReqRep(t *testing.T) {
	s := startServer(t, 5007)
	defer s.Stop()
	c, _ := Dial(":5007", client_conf)
	defer c.Close()
	// make some requests
	concurrency := 10
	req_duration := time.Millisecond * 100
	max_allowed_elapsed := req_duration * 2
	replies := make(chan int, concurrency)
	t0 := time.Now()
	for i := 0; i < concurrency; i++ {
		j := i
		go func() {
			MkReq(t, c, j, req_duration, req_duration+req_duration/2, nil)
			replies <- j
		}()
	}
	// wait for results
	collected := 0
	for i := 0; i < concurrency; i++ {
		select {
		case <-replies:
			collected++
		case <-time.After(max_allowed_elapsed):
		}
	}
	if collected != concurrency {
		t.Errorf(
			"only %d answers collected but expected %d",
			collected, concurrency)
	}
	elapsed := time.Now().Sub(t0)
	// check total duration
	if req_duration < elapsed && elapsed < max_allowed_elapsed {
		// OK
		return
	}
	t.Errorf(
		"elapsed time is %s but expected %s..%s",
		elapsed.String(), req_duration.String(), max_allowed_elapsed.String())
}

/*
 ----------------------------------------------------------------------
  Pool-Servers Tests
 ----------------------------------------------------------------------
*/

func TestPoolReqRepSimple(t *testing.T) {
	s := startServers(t, 5005)
	defer stopServers(s)
	p := startPool(5005)
	defer p.Close()
	// make some requests
	MkReq(t, p, 1, time.Millisecond*20, time.Millisecond*60, nil)
	MkReq(t, p, 2, time.Millisecond*20, time.Millisecond*20, TimeoutError)
}

func TestPoolReqRep(t *testing.T) {
	s := startServers(t, 5004, 5006, 5008)
	defer stopServers(s)
	p := startPool(5004, 5005, 5006, 5007, 5008)
	defer p.Close()
	// make some requests
	for i := 0; i < 10; i++ {
		MkReq(t, p, i, time.Millisecond*20, time.Millisecond*60, nil)
	}
	for i := 0; i < 10; i++ {
		MkReq(t, p, i, time.Millisecond*20, time.Millisecond*20, TimeoutError)
	}
}

func TestPoolBalancing(t *testing.T) {
	s := startDumbServers(t, 5020, 5021, 5022)
	defer stopServers(s)
	p := startPool(5020, 5021, 5022)
	defer p.Close()
	v1, v2 := -1, -1
	for i := 0; i < 10; i++ {
		r := MkReqSimple(t, p)
		if r == v1 || r == v2 {
			t.Errorf("result should not be %d or %d", v1, v2)
		}
		v1 = v2
		v2 = r
	}
}

func TestPoolCast(t *testing.T) {
	s := startServers(t, 5009, 5010)
	defer stopServers(s)
	p := startPool(5009, 5010, 5011, 5012, 5013)
	defer p.Close()
	for i := 0; i < 10; i++ {
		MkCast(t, p, i, nil)
	}
}

func TestPoolParallelReqRep(t *testing.T) {
	s := startServers(t, 5014, 5016, 5018)
	defer stopServers(s)
	p := startPool(5014, 5015, 5016, 5017, 5018)
	defer p.Close()
	// make some requests
	concurrency := 10
	req_duration := time.Millisecond * 100
	max_allowed_elapsed := req_duration * 2
	replies := make(chan int, concurrency)
	t0 := time.Now()
	for i := 0; i < concurrency; i++ {
		j := i
		go func() {
			MkReq(t, p, j, req_duration, req_duration+req_duration/2, nil)
			replies <- j
		}()
	}
	// wait for results
	collected := 0
	for i := 0; i < concurrency; i++ {
		select {
		case <-replies:
			collected++
		case <-time.After(max_allowed_elapsed):
		}
	}
	if collected != concurrency {
		t.Errorf(
			"only %d answers collected but expected %d",
			collected, concurrency)
	}
	elapsed := time.Now().Sub(t0)
	// check total duration
	if req_duration < elapsed && elapsed < max_allowed_elapsed {
		// OK
		return
	}
	t.Errorf(
		"elapsed time is %s but expected %s..%s",
		elapsed.String(), req_duration.String(), max_allowed_elapsed.String())
}

func TestPoolSuspend(t *testing.T) {
	s := startServer(t, 5023)
	defer s.Stop()
	p := startPool(5023, 5023) // both clients to one server
	defer p.Close()
	// make some requests
	MkReq(t, p, 1, time.Millisecond, time.Millisecond*10, nil)
	MkReq(t, p, 2, time.Millisecond, time.Millisecond*10, nil)
	s.Suspend(time.Second)
	time.Sleep(time.Millisecond * 400)
	MkReq(t, p, 3, time.Millisecond, time.Millisecond*10, NotConnectedError)
	time.Sleep(time.Millisecond * 400)
	MkReq(t, p, 4, time.Millisecond, time.Millisecond*10, NotConnectedError)
	time.Sleep(time.Millisecond * 400)
	MkReq(t, p, 5, time.Millisecond, time.Millisecond*10, nil)
}

/*
 ----------------------------------------------------------------------
  Internal functions and definitions
 ----------------------------------------------------------------------
*/

const NONE = 99999999999

var ANY error
var cast_result int
var server_conf ServerConf
var client_conf ClientConf
var pool_conf PoolConf

func init() {
	ANY = errors.New("any error")
	server_conf = NewServerConf()
	f1 := RequestCallback
	server_conf.RequestCallback = &f1
	f2 := CastCallback
	server_conf.CastCallback = &f2
	server_conf.Trace = trace_enabled
	client_conf = NewClientConf()
	client_conf.ReconnectPeriod = time.Millisecond * 5
	client_conf.Trace = trace_enabled
	pool_conf = NewPoolConf()
	pool_conf.ReconnectPeriod = time.Millisecond * 5
	pool_conf.Trace = trace_enabled
	pool_conf.ClientTrace = trace_enabled
}

func startServers(t *testing.T, ports ...uint16) []*Server {
	res := make([]*Server, len(ports))
	for i := 0; i < len(ports); i++ {
		res[i] = startServer(t, ports[i])
	}
	return res
}

func startServer(t *testing.T, port uint16) *Server {
	log.Printf("starting server at %d...", port)
	s, err := Listen(port, server_conf)
	if err != nil {
		t.Fatal(err)
		return nil
	}
	return s
}

func startDumbServers(t *testing.T, ports ...uint16) []*Server {
	res := make([]*Server, len(ports))
	for i := 0; i < len(ports); i++ {
		j := i
		log.Printf("starting dumb server#i at %d...", ports[i])
		server_conf := NewServerConf()
		f1 := func([]byte) []byte {
			reply := make([]byte, 8)
			binary.BigEndian.PutUint64(reply, uint64(j))
			return reply
		}
		server_conf.RequestCallback = &f1
		server_conf.Trace = trace_enabled
		s, err := Listen(ports[i], server_conf)
		if err != nil {
			t.Fatal(err)
		}
		res[i] = s
	}
	return res
}

func stopServers(s []*Server) {
	for i := 0; i < len(s); i++ {
		s[i].Stop()
	}
}

func startPool(ports ...uint16) *Pool {
	peers := make([]string, len(ports))
	for i := 0; i < len(ports); i++ {
		peers[i] = fmt.Sprintf(":%d", ports[i])
	}
	pool_conf.Peers = peers
	p := NewPool(pool_conf)
	time.Sleep(time.Millisecond * 300)
	return p
}

func RequestCallback(data []byte) []byte {
	// decode request
	arg := binary.BigEndian.Uint64(data[0:8])
	duration := time.Duration(binary.BigEndian.Uint64(data[8:]))
	// do main work
	time.Sleep(duration)
	result := arg * 2
	// encode response
	reply := make([]byte, 8)
	binary.BigEndian.PutUint64(reply, result)
	return reply
}

func CastCallback(data []byte) {
	// decode request
	arg := int(binary.BigEndian.Uint64(data[0:8]))
	// do main work
	cast_result = arg * 3
}

func sleep(duration time.Duration) {
	log.Printf("sleeping for %s...", duration.String())
	time.Sleep(duration)
}

func MkReq(t *testing.T, c interface{}, arg int, duration, timeout time.Duration, expected_error error) {
	log.Printf(
		"making request %d of duration %s with timeout %s. Expected result is `%v`...",
		arg, duration.String(), timeout.String(), expected_error)
	// encode request
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], uint64(arg))
	binary.BigEndian.PutUint64(data[8:], uint64(duration))
	// send request
	var reply []byte
	var err error
	t0 := time.Now()
	switch c.(type) {
	case *Client:
		reply, err = (c.(*Client)).Req(data, timeout)
	case *Pool:
		reply, err = (c.(*Pool)).Req(data, timeout)
	}
	elapsed := time.Now().Sub(t0)
	// examine result
	switch expected_error.(type) {
	case nil:
		if err != nil {
			t.Errorf(
				"req %d (duration %s, timeout %s) suddenly failed: %v (took %s)",
				arg, duration.String(), timeout.String(), err, elapsed.String())
			return
		}
		if len(reply) != 8 {
			t.Errorf(
				"req %d (duration %s, timeout %s) gave bad reply len: %d (took %s)",
				arg, duration.String(), timeout.String(), len(reply), elapsed.String())
			return
		}
		answer := int(binary.BigEndian.Uint64(reply))
		if answer != arg*2 {
			t.Errorf(
				"req %d (duration %s, timeout %s) gave bad reply: %d (expected %d) (took %s)",
				arg, duration.String(), timeout.String(), answer, arg*2, elapsed.String())
		}
	default:
		if err == nil {
			t.Errorf(
				"req %d (duration %s, timeout %s) suddenly succeeded with %d (took %s)",
				arg, duration.String(), timeout.String(), reply, elapsed.String())
			return
		}
		if reflect.TypeOf(expected_error) == reflect.TypeOf(ANY) {
			return
		}
		if reflect.TypeOf(err) == reflect.TypeOf(expected_error) {
			return
		}
		t.Errorf(
			"req %d (duration %s, timeout %s) failed but with unexpected"+
				" error `%v` (expected `%v`) (took %s)",
			arg, duration.String(), timeout.String(), err,
			expected_error, elapsed.String())
	}
}

func MkReqSimple(t *testing.T, c interface{}) int {
	log.Printf("making simple request...")
	// send request
	var reply []byte
	var err error
	switch c.(type) {
	case *Client:
		reply, err = (c.(*Client)).Req([]byte{}, time.Millisecond*50)
	case *Pool:
		reply, err = (c.(*Pool)).Req([]byte{}, time.Millisecond*50)
	}
	if err != nil {
		t.Errorf("simple req suddenly failed: %v", err)
		return 0
	}
	res := int(binary.BigEndian.Uint64(reply))
	log.Printf("   answer is %d", res)
	return res
}

func MkCast(t *testing.T, c interface{}, arg int, expected_error error) {
	log.Printf("making cast %d. Expected result is `%v`...", arg, expected_error)
	// encode request
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(arg))
	// send request
	cast_result = NONE
	var err error
	t0 := time.Now()
	switch c.(type) {
	case *Client:
		err = (c.(*Client)).Cast(data)
	case *Pool:
		err = (c.(*Pool)).Cast(data)
	}
	elapsed := time.Now().Sub(t0)
	// examine result
	if expected_error == nil {
		if err != nil {
			t.Errorf("req %d suddenly failed: %v (took %s)", arg, err, elapsed.String())
		}
		// wait until server processes the cast
		time.Sleep(time.Millisecond * 10)
		if cast_result != arg*3 {
			t.Errorf("req %d gave bad answer %d (expected %d). Took %s",
				arg, cast_result, arg*3, elapsed.String())
		}
		return
	}
	if err == nil {
		t.Errorf("req %d suddenly succeeded (took %s)", arg, elapsed.String())
		return
	}
	if reflect.TypeOf(expected_error) == reflect.TypeOf(ANY) {
		return
	}
	if reflect.TypeOf(err) != reflect.TypeOf(expected_error) {
		t.Errorf(
			"req %d failed but with unexpected error %v (expected %v) (took %s)",
			arg, err, expected_error, elapsed.String())
	}
}
