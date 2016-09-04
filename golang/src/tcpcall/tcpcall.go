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

var trace_client bool
var trace_pool bool
var trace_server bool

func init() {
	s := os.Getenv("TCPCALL_CLIENT_TRACE")
	trace_client = 0 < len(s)
	s = os.Getenv("TCPCALL_POOL_TRACE")
	trace_pool = 0 < len(s)
	s = os.Getenv("TCPCALL_SERVER_TRACE")
	trace_server = 0 < len(s)
}
