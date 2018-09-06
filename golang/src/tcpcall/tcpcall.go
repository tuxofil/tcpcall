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

var (
	traceClient    bool
	tracePool      bool
	traceServer    bool
	defConcurrency               = 100
	defWBufSize                  = 0xffff
	defMinFlush    time.Duration = 0
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
		defConcurrency = int(i)
	}
	s := os.Getenv("TCPCALL_CLIENT_TRACE")
	traceClient = 0 < len(s)
	s = os.Getenv("TCPCALL_POOL_TRACE")
	tracePool = 0 < len(s)
	s = os.Getenv("TCPCALL_SERVER_TRACE")
	traceServer = 0 < len(s)
	if s := os.Getenv("TCPCALL_W_BUF_SIZE"); 0 < len(s) {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("bad value %#v for"+
				" write buffer size: %s", s, err)
		}
		defWBufSize = int(i)
	}
	if s := os.Getenv("TCPCALL_W_BUF_MIN_FLUSH_PERIOD"); 0 < len(s) {
		d, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("bad value %#v for write"+
				" buffer min flush period: %s", s, err)
		}
		defMinFlush = d
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
