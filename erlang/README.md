# TCP-based Request-Reply Bridge for Erlang nodes.

## Summary

Tcpcall is an Erlang application and API which enables Request-Reply
and Request-only (responseless) interactions, over a dedicated TCP/IP
network connection, between isolated Erlang nodes (i.e., nodes need
not be connected into a cluster).

The tcpcall bridge does not do any encoding/decoding of the payload
data and expects request and reply data to be binaries.  This is done
to avoid excessive copying of potentially big Erlang terms between
processes.

The bridge consists of two components: *client* and *server*.  The
server is listening for incoming TCP connections on a configured
port. The client connects to the server.  Requests can be sent only
from client to server.

A request from the client, once transferred over the network to the
server side, is relayed to a *receiver*.  The receiver can be defined
as an arbitrary Erlang process which will receive special messages, or
as a functional object, which will be applied in a new Erlang process
each time a new request arrives.

Communication between client and server is implemented in such a way
as not to block until a reply from the server arrives.  The same
**tcpcall** connection can be used by multiple Erlang processes
simultaneously -- all requests will be multiplexed on the client side
before being sent to the server.  Similarly, server replies will be
demultiplexed and sent back to appropriate caller processes.  This is
where **tcpcall** is different from ØMQ REQ-REP: the latter will fail
when you try to send two simultaneous requests to the socket, but
**tcpcall** will not.

The client side part will automatically reconnect when the TCP
connection is closed by the another side.  All calls to
``tcpcall:call/3`` and ``tcpcall:cast/2`` will return ``{error,
not_connected}`` until the client reconnects to the server (there is
no queuing of client messages).

For more API details see examples below and a description
of the ``tcpcall`` Erlang module.

## Main features

* easy and efficient way to build RPC-like or send-only interactions
 between Erlang nodes without the need to connect the nodes
 into an Erlang cluster;
* flow control: the server can advise clients to hold off sending
 new data for a configurable period of time;
* no introspection, conversion or data en-/decoding: data are received
 and sent as is, in the form of plain binaries (and only binaries).

## Example with message passing

On the server node:

```erlang
{ok, Pid} = tcpcall:listen([{bind_port, 5000}, {receiver, self()}]),
...
receive
    {tcpcall_req, BridgePid, RequestRef, EncodedRequest} ->
        Request = binary_to_term(EncodedRequest),
        Reply = Request * 2,
        EncodedReply = term_to_binary(Reply),
        ok = tcpcall:reply(BridgePid, RequestRef, EncodedReply);
    {tcpcall_cast, BridgePid, EncodedRequest} ->
        Request = binary_to_term(EncodedRequest),
        %% do something with request
        ...
```

On the client node:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} = tcpcall:call(Pid, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),

EncodedCast = term_to_binary({my_sophisticated_cast, 5, ["abc", make_ref()]}),
ok = tcpcall:cast(Pid, EncodedCast),
...
```

## Example with a callback function

On the server node:

```erlang
{ok, Pid} =
    tcpcall:listen(
        [{bind_port, 5000},
         {receiver,
          fun(Request) ->
              case binary_to_term(Request) of
                  Integer when is_integer(Integer) ->
                      term_to_binary(Integer * 2)
                  Cast ->
                      %% do something with cast request
                      ...
              end
          end}]),
...
```

On the client node:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} = tcpcall:call(Pid, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),

EncodedCast = term_to_binary({my_sophisticated_cast, 5, ["abc", make_ref()]}),
ok = tcpcall:cast(Pid, EncodedCast),
...
```

Note that when you use a functional object for processing casts (asynchronous
requests not expecting a response), the return value of the function will be silently
discarded.

## Client and server as part of the supervision tree

Here is an example of starting **tcpcall** server as part of the supervision tree of
your Erlang application:

```erlang
%% @hidden
%% @doc Callback for application supervisor.
init(_Args) ->
    {ok, {
       {one_for_one, 5, 1},
       [
        ...
        {tcpcall_server,
         {tcpcall, listen, [[{name, my_server},
                             {bind_port, 5001},
                             {receiver, fun mymod:process_request/1}
                            ]]},
         permanent, brutal_kill, worker, [tcpcall]},
        ...
       ]
      }}.
```

And, here is how you start a **tcpcall** client as part of the supervision tree:

```erlang
%% @hidden
%% @doc Callback for application supervisor.
init(_Args) ->
    {ok, {
       {one_for_one, 5, 1},
       [
        ...
        {tcpcall_client,
         {tcpcall, connect, [[{name, my_client}, {host, "10.0.0.1"}, {port, 5001}]]},
         permanent, brutal_kill, worker, [tcpcall]},
        ...
       ]
      }}.
```

Now you can make **tcpcall** client calls from any process of your
application like so:

```erlang
...
case tcpcall:call(my_client, Request, Timeout) of
    {ok, Reply} ->
        ...;
    {error, timeout} ->
        %% remote side doesn't respond within timeout
        ...;
    {error, overload} ->
        %% tcpcall client overloaded with incoming requests
        ...;
    {error, not_connected} ->
        %% connection to server is not alive
        ...
    {error, OtherError} ->
        %% something bad happen (network error or remote request processor crashed)
        ...
end,
...
```

or send casts:

```erlang
...
case tcpcall:cast(my_client, Request) of
    ok ->
        ...;
    {error, overload} ->
        %% tcpcall client overloaded with incoming requests
        ...;
    {error, not_connected} ->
        %% connection to server is not alive
        ...
    {error, OtherError} ->
        %% something bad happen (network error)
        ...
end,
...
```

## Using connection pools for load balancing

Suppose you have the following **tcpcall** servers on nodes:

* 10.0.0.1:5001;
* 10.0.0.2:5002;
* 10.0.0.3:5003.

You can now set up a pool of **tcpcall** clients for these
servers. The pool will balance your requests between all alive
servers. When a request fails to be sent to a server, another server
will be tried until no alive servers are left or a user-defined
``Timeout`` is reached (see ``tcpcall:call_pool/3`` API function).

You can choose one of the two available balancing policies:

* ``round_robin``;
* ``random``.

**tcpcall** connection pools support auto reconfiguration.
A pool can be configured to re-read and apply on-the-fly a list of servers
using a custom functional object. New connections will be established
in background and added to the pool, and alive connections to the servers
not listed in new configurations, will be removed from the pool and closed.

Lets try to start a connection pool:

```erlang
...
{ok, _Pid} =
    tcpcall:connect_pool(
        my_pool,
        [{balancer, round_robin},
         {peers, [{"10.0.0.1", 5001},
                  {"10.0.0.2", 5002},
                  {"10.0.0.3", 5003}]}]),
```

You can also embed the pool in your Erlang application supervision
tree, as follows:

```erlang
%% @hidden
%% @doc Callback for application supervisor.
init(_Args) ->
    ReconfigPeriod = 30, %% in seconds
    {ok, {
       {one_for_one, 5, 1},
       [
        ...
        {tcpcall_pool,
         {tcpcall, connect_pool, [my_pool,
                                  [{peers, fun get_servers/0, ReconfigPeriod}]]},
         permanent, brutal_kill, worker, [tcpcall]},
        ...
       ]
      }}.

-spec get_servers() ->
           [{Host :: inet:ip_address() | atom() | string() | binary(),
             Port :: inet:port_number()}].
get_servers() ->
    [{"10.0.0.1", 5001},
     {"10.0.0.2", 5002},
     {"10.0.0.3", 5003}].
```

Now let's use the pool to balance requests to the servers:

```erlang
...
{ok, EncodedReply} = tcpcall:call_pool(my_pool, EncodedRequest1, Timeout),
...
ok = tcpcall:cast_pool(my_pool, EncodedRequest2),
...
```

We can even reconfigure the pool manually, adding and removing servers:

```erlang
...
NewPoolOptions =
    [{balancer, random},
     {peers, [{"10.0.0.1", 5001},
              {"10.0.0.3", 5003},
              {"10.0.0.4", 5003},
              {"10.0.0.5", 5003}
             ]}],
ok = tcpcall:reconfig_pool(my_pool, NewPoolOptions),
...
```

And finally, stop the pool:

```erlang
...
ok = tcpcall:stop_pool(my_pool),
...
```

## Flow control examples

At any time a **tcpcall** server can send a ``suspend`` request back
to all connected clients. This is an indication for the clients to
suspend further sending of data (which they can still choose to
ignore). This feature can be used to avoid overload of the server.

Clients should use the ``suspend_handler`` option to specify the
process which will receive ``suspend`` signals from the server:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000},
                             {suspend_handler, self()}]),
...
%% send some data
ok = tcpcall:cast(Pid, Request1),
ok = tcpcall:cast(Pid, Request2),
ok = tcpcall:cast(Pid, Request3),
...
%% check for suspend
receive
    {tcpcall_suspend, Pid, Millis} ->
        ok = timer:sleep(Millis)
after 0 ->
    ok
end,
%% send data again
ok = tcpcall:cast(Pid, Request4),
ok = tcpcall:cast(Pid, Request5),
ok = tcpcall:cast(Pid, Request6),
...
```

Alternatively, the value of ``suspend_handler`` can be a functional
object with an arity of 1:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000},
                             {suspend_handler, fun suspend_handler/1}]),
```

Here is an example of a suspend handler function:

```erlang
-spec suspend_handler(Millis :: non_neg_integer()) -> Ignored :: any().
suspend_handler(Millis) ->
    %% do something
    ...
    ok.
```

This function will be called automatically for each ``suspend`` request received
from server side without any efforts from the **tcpcall** library user.

Now, let's go back to the server side of **tcpcall** connection. A
server function which receives and processes requests, can signal to
the clients to delay sending more data by using a ``suspend`` function:

```erlang
...
ok = tcpcall:suspend(ServerPidOrName, Millis),
...
```

Appropriate signal will be sent to all connected clients.

### Early discard for suspend mode

Consider the following use case:

1. Clients have sent 1000 requests to one server in a short period of time;
2. The server sees it can't process all these requests right now and
 sends a ``suspend``  signal back to the clients, asking to wait with
 new requests for the next 5 minutes;
3. Clients receive the signal and stop sending more data, waiting for 5 minutes to elapse;
4. Within 2 minutes, the server has managed to process all buffered
 requests.

In order to avoid wasting 3 remaining minutes, the server can call
``tcpcall:resume/1``. A special ``resume`` signal will be sent to all
connected clients so they can proceed with sending their requests
immediately.

To handle ``resume`` signals on the client side, you have to define
``resume_handler`` option when creating **tcpcall** client:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000},
                             {suspend_handler, fun suspend_handler/1},
                             {resume_handler, ResumeHandler}]),
```

where ``ResumeHandler`` can be an Erlang process ID, Erlang process
registered name or a functional object with an arity of 0. If
``ResumeHandler`` is a registered name or PID, that process will
receive a message like:

```erlang
receive
    {tcpcall_resume, ClientPID} ->
        ...
end,
```

If ``ResumeHandler`` is a functional object, it can be defined as follows:

```erlang
ResumeHandler =
    fun() ->
        %% do something here when 'resume' signal
        %% arrives from the tcpcall server side
        ...
    end,
```

The term returned by the ``ResumeHandler`` callback is ignored.

### Flow control in connection pools

It's even easier than with bare **tcpcall** connections. There is nothing to do
on the client side, just create connection pool as usual.

When one of your servers decides to call ``tcpcall:suspend/2``, the
pool receives this signal and removes that server from load balancing
for a requested period of time.  When the suspend period expires, the
server will be added back to the load balancing automatically. Suspend
mode for a server will be cancelled immediately when it sends a
``resume`` signal.

When you try to send too much data and all servers go into suspend mode, all
``tcpcall:call_pool/3`` and ``tcpcall:cast_pool/2`` requests will return
``{error, not_connected}``, advising the caller to retry later.

## Sending async data from server back to client

The previous section covered sending flow control signals from server
to connected clients.

There is another method which can be used to send any data from
the server to all connected clients:

```erlang
Encoded = term_to_binary({any, term, you, want, [1, 2, 3]}),
ok = tcpcall:uplink_cast(ServerRef, Encoded),
```

In order to receive such cast messages on the client side, you have to
create your client process as follows:

```erlang
{ok, _} = tcpcall:connect([{host, Host}, {port, Port}, {name, c1},
                           {uplink_cast_handler, UplinkCastHandler}]),
```

where ``UplinkCastHandler`` can be:

* An Erlang process registered name (atom);
* An Erlang process ID;
* A functional object of arity 1.

When ``UplinkCastHandler`` is set to be an atom or PID, the target
process will receive a message like:

```erlang
receive
    {tcpcall_uplink_cast, ConnectionPID, Encoded} ->
        Cast = binary_to_term(Encoded),
        %% do something with request
        ...
end,
```

When ``UplinkCastHandler`` is a functional object, the function it
refers to should be like this one:

```erlang
-spec uplink_cast_handler(Encoded :: binary()) -> Ignored :: any().
uplink_cast_handler(Encoded) ->
    Cast = binary_to_term(Encoded),
    %% do something with request
    ...
    ok.
```

### Handling async data from server with pools

To be able to receive such data, the pool must be created with
these configuration options:

```erlang
{ok, _} = connect_pool(p1, [{peers, Peers},
                            {uplink_cast_handler, UplinkCastHandler}]),
```

Unlike ``suspend`` and ``resume`` signals, **tcpcall** connection pools don't
handle uplink casts themselves. If created without an ``uplink_cast_handler`` option,
the pool will silently discard all data sent by the servers.
