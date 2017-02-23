/*
Error definitions.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

import "errors"

var (
	NotConnectedError  = errors.New("not connected")
	OverloadError      = errors.New("client overloaded")
	DisconnectedError  = errors.New("disconnected")
	TimeoutError       = errors.New("request timeout")
	RemoteCrashedError = errors.New("remote site crashed")
	ConnectError       = errors.New("connect failed")
)
