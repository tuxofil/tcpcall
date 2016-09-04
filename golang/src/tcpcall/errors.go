/*
Error definitions.

Author: Aleksey Morarash <aleksey.morarash@gmail.com>
Since: 4 Sep 2016
Copyright: 2016, Aleksey Morarash <aleksey.morarash@gmail.com>
*/

package tcpcall

type NotConnectedError struct{}

func (e NotConnectedError) Error() string { return "not connected" }

type OverloadError struct{}

func (e OverloadError) Error() string { return "client overloaded" }

type DisconnectedError struct{}

func (e DisconnectedError) Error() string { return "disconnected" }

type TimeoutError struct{}

func (e TimeoutError) Error() string { return "request timeout" }

type RemoteCrashedError struct{ text string }

func (e RemoteCrashedError) Error() string { return e.text }

type ConnectError struct{}

func (e ConnectError) Error() string { return "connect failed" }
