# TCP-based Request-Reply Bridge for Erlang nodes.

## Summary

Provides an API to make Request-Reply interactions between
an Erlang nodes using the TCP/IP network protocol.

The bridge does not any encoding/decoding of the payload
data and assumes request and reply is given as binaries.
This is done to reduce memory copies of a potentially big
Erlang terms between processes.

The bridge consists of two sides: client and server.
The server is listening for incoming TCP connections on a
configured port number. The client connects to the server.
A request can be send only from the client to the server.

The request from the client, once transferred through the
network to the server side, is relayed to a so called
receiver. The receiver can be defined as arbitrary Erlang
process which will receive special messages or as a functional
object, which will be applied in a new Erlang process each
time when new request arrives.

The client side part does automatic reconnect when TCP connection
closed from the another side. The time until client will
reconnect to the server all calls to tcpcall:call/3 will
return {error, not_connected}.

For more API details see examples below and a description
of the tcpcall Erlang module.

## Main features

* easy and efficient way to build RPC-like interactions
 between two Erlang nodes without need to bind the nodes
 into an Erlang cluster;
* does not any complex data processing, as it operates only
 with binaries (the user must implement payload encoding and
 deconding by himself).

## Example with message passing

On the server node:

```
{ok, Pid} = tcpcall:listen([{bind_port, 5000}, {receiver, self()}]),
...
receive
    {tcpcall_req, BridgePid, RequestRef, EncodedRequest} ->
        Request = binary_to_term(EncodedRequest),
        Reply = Request * 2,
        EncodedReply = term_to_binary(Reply),
        ok = tcpcall:reply(BridgePid, RequestRef, EncodedReply),
        ...
```

On the client node:

```
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} = tcpcall:call(Pid, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),
...
```

== Example with callback function ==

On the server node:

```
{ok, Pid} =
    tcpcall:listen(
        [{bind_port, 5000},
         {receiver,
          fun(Request) ->
              term_to_binary(binary_to_term(Request) * 2)
          end}]),
...
```

On the client node:

```
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} = tcpcall:call(Pid, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),
...
```
