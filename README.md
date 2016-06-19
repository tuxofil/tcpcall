# TCP-based Request-Reply Bridge for Erlang nodes.

## Summary

Provides an API to make Request-Reply and Request-only
(responseless) interactions between an Erlang nodes using the
TCP/IP network protocol.

The bridge does not any encoding/decoding of the payload
data and assumes request and reply are given as binaries.
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

Communication between client and server implemented in such
way to not block until reply from the server will arrive.
You can use same tcpcall connection from many Erlang processes
simultaneously - all requests will be multiplexed on the
client side before sending them to the server. All server
replies will be demultiplexed and sent back to appropriate
caller processes. This is the thing which differs tcpcall
from ØMQ REQ-REP. Latter will fail when you try to send
two simultaneous requests to the socket, but tcpcall will not.

The client side part does automatic reconnect when TCP connection
closed from the another side. The time until client will
reconnect to the server all calls to tcpcall:call/3 and
tcpcall:cast/2 will return {error, not_connected}.

For more API details see examples below and a description
of the tcpcall Erlang module.

## Main features

* easy and efficient way to build RPC-like or send-only interactions
 between two Erlang nodes without need to bind the nodes
 into an Erlang cluster;
* flow control - server side can inform clients to stop sending
 new data for a configurable period of time;
* does not any complex data processing, as it operates only
 with binaries (the user must implement payload encoding and
 deconding by himself).

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

## Example with callback function

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

Note when you use functional object for processing casts (asynchronous
requests without a response), return of the function will be silently
discarded.

## Client and server as part of the supervision tree

Here is example for starting tcpcall server as part of the supervision tree of
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

Here is example for starting tcpcall client as part of the supervision tree of
your Erlang application:

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

Now you can use tcpcall client from any process of your application like:

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

or send casts like:

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

Suppose you have a few tcpcall servers on nodes:

* 10.0.0.1:5001;
* 10.0.0.2:5002;
* 10.0.0.3:5003.

You can start pool of tcpcall clients for these servers. The pool will
balance your requests between all alive servers. In case when request was
failed to send to one server, it will be sent to next one until no alive
servers left. Failover is done in such way to not exceed Timeout, passed
as third argument to tcpcall:call_pool/3 API function.

You can choose one of two available balancing policies:

* round_robin;
* random.

Convenient feature of tcpcall connection pools is auto reconfiguration.
Pool can be configured to re-read and apply on-the-fly list of servers
using your custom functional object. New connections will be established
in background and added to the pool, and alive connections to the servers
not listed in new configurations, will be removed from the pool and closed.

Lets try to start the pool:

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

Certainly, you can embed the pool in your Erlang application supervision
tree like follows:

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

Now lets use the pool to balance requests to the servers:

```erlang
...
{ok, EncodedReply} = tcpcall:call_pool(my_pool, EncodedRequest1, Timeout),
...
ok = tcpcall:cast_pool(my_pool, EncodedRequest2),
...
```

And even reconfigure the pool manually, adding and removing servers:

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

In any time tcpcall server side can send 'suspend' request back to all
connected clients. Clients can handle such request and suspend data send
(or can ignore such signals at all). This feature can be used to avoid
overload of the server.

Client side should use 'suspend_handler' option to able to react on
'suspend' signals from the server:

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

Value for 'suspend_handler' option can refer functional object with arity 1:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000},
                             {suspend_handler, fun suspend_handler/1}]),
```

Here is an example of suspend handler function:

```erlang
-spec suspend_handler(Millis :: non_neg_integer()) -> Ignored :: any().
suspend_handler(Millis) ->
    %% do something
    ...
    ok.
```

This function will be called automatically for each 'suspend' request received
from server side without any efforts from the tcpcall library user.

So, lets back to the server side of tcpcall connection. Say, server processor
function which receives requests can ask for suspend as follows:

```erlang
...
ok = tcpcall:suspend(ServerPidOrName, Millis),
...
```

Appropriate signal will be sent to all connected clients.

### Early discard for suspend mode

Consider use case:

1. Clients sent 1000 requests to the one server in a short period of time;
2. The server see he can't process all these requests right now and send 'suspend'
 signal back to the clients, asking for suspend for next 5 minutes;
3. Clients receive the signal and stop to send new data, waiting for 5 minutes to elapse;
4. Only 2 minutes was elapsed, but server has been already processed all buffered
 requests.

What should we do? Idle for next 3 minutes? There's a better solution - server
side calls tcpcall:resume/1 API call. Special 'resume' signal will be dispatched
to all connected clients so they can discard suspend mode immediately.

To handle 'resume' signals on the client side you have to define 'resume_handler'
option when creating tcpcall client:

```erlang
{ok, Pid} = tcpcall:connect([{host, "server.com"}, {port, 5000},
                             {suspend_handler, fun suspend_handler/1},
                             {resume_handler, ResumeHandler}]),
```

where ResumeHandler can be Erlang process ID, Erlang process registered name
or functional object with arity of 0. If ResumeHandler is registered name or PID,
target process will receive messages like:

```erlang
receive
    {tcpcall_resume, ClientPID} ->
        ...
end,
```

If ResumeHandler is refer to a functional object, it can be defined as follows:

```erlang
ResumeHandler =
    fun() ->
        %% do something here when 'resume' signal
        %% arrives from the tcpcall server side
        ...
    end,
```

Term, returned by the ResumeHandler, is ignored.

### Flow control in connection pools

It's even easier than with bare tcpcall connections. There is nothing to do
on the client side, just create connection pool as usual.

When one of your servers decides to call tcpcall:suspend/2, the pool receives
this signal and removes that server from load balancing for requested period
of time. When suspend period expires, server will be added to the load balancing
again automatically.

When you try to send to much data and all servers go to suspend mode, all
tcpcall:call_pool/3 and tcpcall:cast_pool/2 requests will return {error, not_connected},
informing caller to retry send later.
