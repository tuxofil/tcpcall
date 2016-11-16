/*
TCP Request-Reply Bridge.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import (
	"os"
)

var (
	traceClient bool
	tracePool   bool
	traceServer bool
)

func init() {
	s := os.Getenv("TCPCALL_CLIENT_TRACE")
	traceClient = 0 < len(s)
	s = os.Getenv("TCPCALL_POOL_TRACE")
	tracePool = 0 < len(s)
	s = os.Getenv("TCPCALL_SERVER_TRACE")
	traceServer = 0 < len(s)
}
