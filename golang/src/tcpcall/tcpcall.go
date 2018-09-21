/*
TCP Request-Reply Bridge.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Global configurations
var (
	// Enable tracing for client or not
	gTraceClient bool
	// Enable tracing for clients pool or not
	gTracePool bool
	// Enable tracing for server or not
	gTraceServer bool
	// For client and clients pool - max simultaneous
	// requests per one connection
	gConcurrency = 100
	// Max write buffer size for connection
	gWriteBufferSize = 0xffff
	// Min flush period for connection write buffer.
	// 0 means data will be written to the socket
	// immediately.
	gMinFlushPeriod time.Duration
	// Channel to hold
	dBufSize = 1000
	bChan    chan [][]byte
)

func init() {
	if s := os.Getenv("TCPCALL_CONCURRENCY"); s != "" {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("bad value %#v for"+
				" write buffer size: %s", s, err)
		}
		gConcurrency = int(i)
	}
	s := os.Getenv("TCPCALL_CLIENT_TRACE")
	gTraceClient = 0 < len(s)
	s = os.Getenv("TCPCALL_POOL_TRACE")
	gTracePool = 0 < len(s)
	s = os.Getenv("TCPCALL_SERVER_TRACE")
	gTraceServer = 0 < len(s)
	if s := os.Getenv("TCPCALL_W_BUF_SIZE"); 0 < len(s) {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("bad value %#v for"+
				" write buffer size: %s", s, err)
		}
		gWriteBufferSize = int(i)
	}
	if s := os.Getenv("TCPCALL_W_BUF_MIN_FLUSH_PERIOD"); 0 < len(s) {
		d, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("bad value %#v for write"+
				" buffer min flush period: %s", s, err)
		}
		gMinFlushPeriod = d
	}

	if s := os.Getenv("TCPCALL_PAYLOAD_BUF_SIZE"); 0 < len(s) {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("bad value %#v for payload"+
				" buffer pool size: %s", s, err)
		}
		dBufSize = int(i)
	}
	bChan = make(chan [][]byte, dBufSize)
}
