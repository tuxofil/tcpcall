%%% @doc
%%% TCP Request-Reply Bridge main interface module.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 10 Nov 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall).

%% API exports
-export(
   [listen/1,
    connect/1,
    is_connected/1,
    connect_pool/2,
    call/3,
    call_pool/3,
    cast/2,
    cast_pool/2,
    is_pool_connected/1,
    reconfig_pool/2,
    reply/3,
    stop_server/1,
    stop_client/1,
    stop_pool/1
   ]).

-include("tcpcall.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

-export_type(
   [listen_options/0,
    listen_option/0,
    client_options/0,
    client_option/0,
    client_pool_name/0,
    client_pool_options/0,
    client_pool_option/0,
    peer/0,
    peers_getter/0,
    balancer/0,
    host/0,
    receiver/0,
    bridge_ref/0,
    data/0
   ]).

-type listen_options() :: [listen_option()].

-type listen_option() ::
        {bind_port, inet:port_number()} |
        {receiver, receiver()} |
        {name, RegisteredName :: atom()}.

-type client_options() :: [client_option()].

-type client_option() ::
        {host, RemoteHost :: host()} |
        {port, RemotePort :: inet:port_number()} |
        {name, RegisteredName :: atom()} |
        {lord, pid()}.

-type client_pool_name() :: atom().

-type client_pool_options() :: [client_pool_option()].

-type client_pool_option() ::
        {peers, [peer()]} |
        {peers, peers_getter(), Seconds :: pos_integer()} |
        {balancer, balancer()}.

-type peer() ::
        {host(), inet:port_number()}.

-type peers_getter() :: fun(() -> [peer()]).

-type balancer() :: ?round_robin | ?random.

-type host() ::
        inet:hostname() |
        inet:ip_address().

-type receiver() ::
        (RegisteredName :: atom()) |
        pid() |
        fun((Request :: data()) -> Reply :: data()).

-type bridge_ref() ::
        (BridgeRegisteredName :: atom()) |
        (BridgePid :: pid()).

-type data() :: binary().

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start server process as part of the supervision tree.
-spec listen(listen_options()) ->
                    {ok, Pid :: pid()} | ignore |
                    {error, Reason :: any()}.
listen(Options) ->
    tcpcall_acceptor:start_link(Options).

%% @doc Start client process as part of the supervision tree.
-spec connect(client_options()) ->
                     {ok, Pid :: pid()} | ignore |
                     {error, Reason :: any()}.
connect(Options) ->
    tcpcall_client:start_link(Options).

%% @doc Check if client is connected or not.
-spec is_connected(BridgeRef :: bridge_ref()) -> boolean().
is_connected(BridgeRef) ->
    tcpcall_client:is_connected(BridgeRef).

%% @doc Start pool of client connections as part of the
%% supervision tree.
%% The pool can be configured as round robin or random
%% (configurable) load balancer.
%% Pool name can be used then with tcpcall:call_pool/3
%% function. The call will be automatically relayed to
%% the one of started tcpcall clients within the pool.
%% Note the pool name will be used as registered name
%% of the process and as a name of named ETS table.
-spec connect_pool(PoolName :: client_pool_name(),
                   Options :: client_pool_options()) ->
                          {ok, Pid :: pid()} | ignore |
                          {error, Reason :: any()}.
connect_pool(PoolName, Options) ->
    tcpcall_pool:start_link(PoolName, Options).

%% @doc Make a Request-Reply interaction with a remote server.
-spec call(BridgeRef :: bridge_ref(),
           Request :: data(),
           Timeout :: pos_integer()) ->
                  {ok, Reply :: any()} |
                  {error, Reason :: any()}.
call(BridgeRef, Request, Timeout)
  when is_binary(Request) ->
    RequestRef = make_ref(),
    DeadLine = tcpcall_lib:micros() + Timeout * 1000,
    case tcpcall_client:queue_request(
           BridgeRef, self(), RequestRef, DeadLine, Request) of
        ok ->
            tcpcall_client:wait_reply(RequestRef, Timeout);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Make a Request-Reply interaction with a remote
%% server through the started connection pool.
%% See connect_pool/2 function description for more details.
-spec call_pool(PoolName :: client_pool_name(),
                Request :: data(),
                Timeout :: pos_integer()) ->
                       {ok, Reply :: any()} |
                       {error, Reason :: any()}.
call_pool(PoolName, Request, Timeout) ->
    tcpcall_pool:call(PoolName, Request, Timeout).

%% @doc Make a asynchronous request with a remote server
%% without waiting for a response.
-spec cast(BridgeRef :: bridge_ref(), Request :: data()) ->
                  ok | {error, Reason :: any()}.
cast(BridgeRef, Request) when is_binary(Request) ->
    RequestRef = make_ref(),
    case tcpcall_client:queue_cast(
           BridgeRef, self(), RequestRef, Request) of
        ok ->
            tcpcall_client:wait_cast_ack(RequestRef);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Make an asynchronous (without a respone) interaction with a remote
%% server through the started connection pool.
%% See connect_pool/2 function description for more details.
-spec cast_pool(PoolName :: client_pool_name(), Request :: data()) ->
                       ok | {error, Reason :: any()}.
cast_pool(PoolName, Request) ->
    tcpcall_pool:cast(PoolName, Request).

%% @doc Check if client pool is connected or not.
%% Return 'true' if at least one of the pool workers is
%% connected to the remote side.
-spec is_pool_connected(PoolName :: client_pool_name()) -> boolean().
is_pool_connected(PoolName) ->
    tcpcall_pool:workers(PoolName) /= [].

%% @doc Apply new configuration for client pool. You
%% can provide new peers or remove existing ones, change
%% balancing strategy.
-spec reconfig_pool(PoolName :: client_pool_name(),
                    NewOptions :: client_pool_options()) -> ok.
reconfig_pool(PoolName, NewOptions) ->
    tcpcall_pool:reconfig(PoolName, NewOptions).

%% @doc Reply to a request from a remote side.
-spec reply(BridgeRef :: bridge_ref(),
            RequestRef :: reference(),
            Reply :: data()) -> ok.
reply(BridgeRef, RequestRef, Reply) when is_binary(Reply) ->
    ok = tcpcall_server:queue_reply(BridgeRef, RequestRef, Reply).

%% @hidden
%% @doc Tell the server process to stop.
%% It is not a part of public API.
-spec stop_server(BridgeRef :: bridge_ref()) -> ok.
stop_server(BridgeRef) ->
    ok = tcpcall_acceptor:stop(BridgeRef).

%% @hidden
%% @doc Tell the client process to stop.
%% It is not a part of public API.
-spec stop_client(BridgeRef :: bridge_ref()) -> ok.
stop_client(BridgeRef) ->
    ok = tcpcall_client:stop(BridgeRef).

%% @hidden
%% @doc Tell the pool process to stop.
%% It is not a part of public API.
-spec stop_pool(PoolName :: client_pool_name()) -> ok.
stop_pool(PoolName) ->
    ok = tcpcall_pool:stop(PoolName).

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% ----------------------------------------------------------------------
%% EUnit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

msg_psng_test() ->
    %% start the bridge
    {ok, _} = listen([{bind_port, 5000}, {receiver, self()}, {name, s}]),
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c}]),
    ok = timer:sleep(500),
    %% spawn the caller process
    Caller =
        spawn_link(
          fun() ->
                  ?assertMatch({ok, 15}, tcall(c, 5, 1000))
          end),
    MonitorRef = monitor(process, Caller),
    %% awaiting for the request
    receive
        {tcpcall_req, BridgeRef, RequestRef, Request} ->
            reply(BridgeRef, RequestRef,
                  term_to_binary(binary_to_term(Request) * 3))
    after 3000 ->
            throw({no_request_from_client})
    end,
    %% awaiting for the caller to finish
    receive
        {'DOWN', MonitorRef, process, Caller, _Reason} ->
            ok
    after 3000 ->
            throw(timeout_waiting_for_caller)
    end,
    %% stop the bridge
    ok = stop_server(s),
    ok = stop_client(c),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

cast_passing_test() ->
    %% start the bridge
    {ok, _} = listen([{bind_port, 5000}, {receiver, self()}, {name, s}]),
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c}]),
    ok = timer:sleep(500),
    %% spawn the caller process
    ?assertMatch(ok, tcast(c, 5)),
    %% awaiting for the request
    receive
        {tcpcall_cast, _BridgeRef, EncodedRequest} ->
            ?assertMatch(5, binary_to_term(EncodedRequest))
    after 3000 ->
            throw({no_request_from_client})
    end,
    %% stop the bridge
    ok = stop_server(s),
    ok = stop_client(c),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

fun_obj_test_() ->
    {setup,
     _Setup =
         fun() ->
                 {ok, Server1} =
                     listen(
                       [{bind_port, 5000},
                        {receiver,
                         fun(EncodedRequest) ->
                                 Request = binary_to_term(EncodedRequest),
                                 Reply = Request * 2,
                                 term_to_binary(Reply)
                         end}]),
                 {ok, Server2} =
                     listen(
                       [{bind_port, 5001},
                        {receiver,
                         fun(EncodedRequest) ->
                                 Request = binary_to_term(EncodedRequest),
                                 ok = timer:sleep(1000),
                                 Reply = Request * 4,
                                 term_to_binary(Reply)
                         end}]),
                 {ok, Client1} =
                     connect(
                       [{host, "127.1"}, {port, 5000},
                        {name, c1}]),
                 {ok, Client2} =
                     connect(
                       [{host, "127.1"}, {port, 5001},
                        {name, c2}]),
                 {ok, Client3} =
                     connect(
                       [{host, "127.1"}, {port, 5002},
                        {name, c3}]),
                 {Server1, Server2, Client1, Client2, Client3}
         end,
     _CleanUp =
         fun({Server1, Server2, Client1, Client2, Client3}) ->
                 ok = stop_server(Server1),
                 ok = stop_server(Server2),
                 ok = stop_client(Client1),
                 ok = stop_client(Client2),
                 ok = stop_client(Client3),
                 %% to avoid port number reuse in other tests
                 ok = timer:sleep(500)
         end,
     [{"Sleep some time until connections will be established...",
       ?_assertMatch(ok, timer:sleep(500))},
      {"Client1 to Server1 request",
       ?_assertMatch({ok, 2}, tcall(c1, 1, 1000))},
      {"Client1 to Server1 request (bad)",
       ?_assertMatch({error, {crashed, _}}, tcall(c1, a, 1000))},
      {"Client2 to Server2 request",
       ?_assertMatch({ok, 4}, tcall(c2, 1, 2000))},
      {"Client2 to Server2 timeouted request",
       ?_assertMatch({error, timeout}, tcall(c2, 1, 500))},
      {"Client3 to nowhere request",
       ?_assertMatch({error, not_connected}, tcall(c3, 1, 1000))}
     ]}.

client_restart_test() ->
    %% start the bridge
    {ok, _} = listen([{bind_port, 5000}, {name, s},
                      {receiver,
                       fun(R) ->
                               term_to_binary(
                                 binary_to_term(R) * 2)
                       end}]),
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c1}]),
    ok = timer:sleep(500),
    ?assertMatch({ok, 4}, tcall(c1, 2, 1000)),
    %% stop the client
    ok = stop_client(c1),
    ok = timer:sleep(500),
    %% ensure the client is stopped
    ?assertMatch(undefined, whereis(c1)),
    %% start a new client
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c2}]),
    ok = timer:sleep(500),
    ?assertMatch({ok, 4}, tcall(c2, 2, 1000)),
    %% stop the bridge
    ok = stop_server(s),
    ok = stop_client(c2),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

server_restart_test() ->
    %% start the bridge
    {ok, _} = listen([{bind_port, 5000}, {name, s1},
                      {receiver,
                       fun(R) ->
                               term_to_binary(
                                 binary_to_term(R) * 2)
                       end}]),
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c}]),
    ok = timer:sleep(500),
    ?assertMatch({ok, 4}, tcall(c, 2, 1000)),
    %% stop the server
    ok = stop_server(s1),
    ok = timer:sleep(500),
    %% ensure the server is stopped
    ?assertMatch(undefined, whereis(s1)),
    %% start a new server
    {ok, _} = listen([{bind_port, 5000}, {name, s2},
                      {receiver,
                       fun(R) ->
                               term_to_binary(
                                 binary_to_term(R) * 3)
                       end}]),
    ok = timer:sleep(500),
    ?assertMatch({ok, 6}, tcall(c, 2, 1000)),
    %% stop the bridge
    ok = stop_server(s2),
    ok = stop_client(c),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

round_robin_pool_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary(b) end}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001}, {"127.1", 5002}]}]),
    ok = timer:sleep(500),
    ?assertMatch({ok, a}, pcall(p1, request, 1000)),
    ?assertMatch({ok, b}, pcall(p1, request, 1000)),
    ?assertMatch({ok, a}, pcall(p1, request, 1000)),
    ?assertMatch({ok, b}, pcall(p1, request, 1000)),
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

round_robin_pool_cast_test() ->
    ServerProcessor = fun() -> receive stop -> ok end end,
    S1 = spawn_link(ServerProcessor),
    S2 = spawn_link(ServerProcessor),
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1}, {receiver, S1}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2}, {receiver, S2}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001}, {"127.1", 5002}]}]),
    ok = timer:sleep(500),
    ?assertMatch(ok, cast_pool(p1, <<"1">>)),
    ?assertMatch(ok, cast_pool(p1, <<"2">>)),
    ?assertMatch(ok, cast_pool(p1, <<"3">>)),
    ?assertMatch(ok, cast_pool(p1, <<"4">>)),
    ok = timer:sleep(500),
    %% test mailboxes of server processors
    ?assertMatch({messages, [{tcpcall_cast, _, <<"1">>},
                             {tcpcall_cast, _, <<"3">>}]},
                 process_info(S1, messages)),
    ?assertMatch({messages, [{tcpcall_cast, _, <<"2">>},
                             {tcpcall_cast, _, <<"4">>}]},
                 process_info(S2, messages)),
    %% stop server processors
    stop = S1 ! stop,
    stop = S2 ! stop,
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

round_robin_pool_failover_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001},
                                         {"127.1", 5002},
                                         %% a bad one:
                                         {"127.1", 5003}
                                        ]}]),
    ok = timer:sleep(500),
    _Ignored =
        [?assertMatch({ok, a}, pcall(p1, request, 1000)) ||
            _ <- lists:seq(1, 1000)],
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

round_robin_pool_cast_failover_test() ->
    ServerProcessor = fun() -> receive stop -> ok end end,
    S1 = spawn_link(ServerProcessor),
    S2 = spawn_link(ServerProcessor),
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1}, {receiver, S1}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2}, {receiver, S2}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001},
                                         {"127.1", 5002},
                                         %% a bad one:
                                         {"127.1", 5003}
                                        ]}]),
    ok = timer:sleep(500),
    [?assertMatch(ok, pcast(p1, request)) || _ <- lists:seq(1, 100)],
    ok = timer:sleep(500),
    %% stop one of the servers and send more data
    ok = stop_server(s1),
    ok = timer:sleep(500),
    [?assertMatch(ok, pcast(p1, request)) || _ <- lists:seq(1, 100)],
    ok = timer:sleep(500),
    %% test server processors mailboxes
    ?assertMatch({message_queue_len, 50},  process_info(S1, message_queue_len)),
    ?assertMatch({message_queue_len, 150}, process_info(S2, message_queue_len)),
    %% stop server processors
    stop = S1 ! stop,
    stop = S2 ! stop,
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

random_pool_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary($a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary($b) end}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001}, {"127.1", 5002}]},
                                {balancer, ?random}]),
    ok = timer:sleep(500),
    Replies =
        [begin
             {ok, X} = pcall(p1, request, 1000), X
         end || _ <- lists:seq(1, 100)],
    ?assertNotEqual(string:copies("ab", 50), Replies),
    ?assertMatch("ab", lists:usort(Replies)),
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

random_pool_failover_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001},
                                         {"127.1", 5002},
                                         %% a bad one:
                                         {"127.1", 5003}
                                        ]}]),
    ok = timer:sleep(500),
    _Ignored =
        [?assertMatch({ok, a}, pcall(p1, request, 1000)) ||
            _ <- lists:seq(1, 1000)],
    %% fetch workers process IDs
    Workers = tcpcall_pool:workers(p1),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500),
    %% check the workers is gone
    lists:foreach(
      fun(Worker) ->
              ?assertNot(is_process_alive(Worker))
      end, Workers).

reconfig_pool_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary(b) end}]),
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, [{"127.1", 5001}]}]),
    ok = timer:sleep(500),
    ok = ?assertMatch({ok, a}, pcall(p1, request, 1000)),
    %% reconfig pool
    ok = reconfig_pool(p1, [{peers, [{"127.1", 5002}]}]),
    ok = timer:sleep(500),
    ok = ?assertMatch({ok, b}, pcall(p1, request, 1000)),
    %% reconfig pool again
    ok = reconfig_pool(p1, [{peers, []}]),
    ok = timer:sleep(500),
    ok = ?assertMatch({error, not_connected}, pcall(p1, request, 1000)),
    %% ...and again
    ok = reconfig_pool(p1, [{peers, [{"127.1", 5001}, {"127.1", 5002}]},
                            {balancer, ?round_robin}]),
    ok = timer:sleep(500),
    Sequence1 =
        [begin {ok, T} = pcall(p1, request, 1000), T end ||
            _ <- lists:seq(1, 10)],
    true = Sequence1 == [a, b, a, b, a, b, a, b, a, b]
        orelse Sequence1 == [b, a, b, a, b, a, b, a, b, a],
    %% ...and again
    ok = reconfig_pool(p1, [{peers, [{"127.1", 5001}, {"127.1", 5002}]},
                            {balancer, ?random}]),
    ok = timer:sleep(500),
    Sequence2 =
        [begin {ok, T} = pcall(p1, request, 1000), T end ||
            _ <- lists:seq(1, 10)],
    true = Sequence2 /= [a, b, a, b, a, b, a, b, a, b]
        andalso Sequence2 /= [b, a, b, a, b, a, b, a, b, a],
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

auto_reconfig_pool_test() ->
    %% start the bridge #1
    {ok, _} = listen([{bind_port, 5001}, {name, s1},
                      {receiver, fun(_) -> term_to_binary(a) end}]),
    %% start the bridge #2
    {ok, _} = listen([{bind_port, 5002}, {name, s2},
                      {receiver, fun(_) -> term_to_binary(b) end}]),
    %% helper cfg table
    ETS = ets:new(c, []),
    true = ets:insert(ETS, {k, [{"127.1", 5001}]}),
    %% Peer list getter function
    Getter = fun() -> [{k, L}] = ets:lookup(ETS, k), L end,
    %% start the pool
    {ok, _} = connect_pool(p1, [{peers, Getter, 1}]),
    ok = timer:sleep(500),
    ok = ?assertMatch({ok, a}, pcall(p1, request, 1000)),
    %% reconfig pool
    true = ets:insert(ETS, {k, [{"127.1", 5002}]}),
    ok = timer:sleep(1100),
    ok = ?assertMatch({ok, b}, pcall(p1, request, 1000)),
    %% stop the bridges
    ok = stop_server(s1),
    ok = stop_server(s2),
    %% stop the pool
    ok = stop_pool(p1),
    %% to avoid port number reuse in other tests
    ok = timer:sleep(500).

is_connected_test() ->
    %% start the client
    {ok, _} = connect([{host, "127.1"}, {port, 5000}, {name, c}]),
    ok = timer:sleep(500),
    ?assertNot(is_connected(c)),
    %% start the server
    {ok, _} = listen([{bind_port, 5000}, {name, s},
                      {receiver, fun(_) -> <<>> end}]),
    ok = timer:sleep(500),
    ?assert(is_connected(c)),
    %% stop the server
    ok = stop_server(s),
    ok = timer:sleep(500),
    ?assertNot(is_connected(c)).

-spec tcall(BridgeRef :: bridge_ref(),
            Request :: any(),
            Timeout :: pos_integer()) ->
                   {ok, Reply :: any()} | {error, Reason :: any()}.
tcall(BridgeRef, Request, Timeout) ->
    case call(BridgeRef, term_to_binary(Request), Timeout) of
        {ok, EncodedReply} ->
            {ok, binary_to_term(EncodedReply)};
        {error, _Reason} = Error ->
            Error
    end.

-spec pcall(PoolName :: client_pool_name(),
            Request :: any(),
            Timeout :: pos_integer()) ->
                   {ok, Reply :: any()} | {error, Reason :: any()}.
pcall(PoolName, Request, Timeout) ->
    case call_pool(PoolName, term_to_binary(Request), Timeout) of
        {ok, EncodedReply} ->
            {ok, binary_to_term(EncodedReply)};
        {error, _Reason} = Error ->
            Error
    end.

-spec tcast(BridgeRef :: bridge_ref(), Request :: any()) ->
                   ok | {error, Reason :: any()}.
tcast(BridgeRef, Request) ->
    cast(BridgeRef, term_to_binary(Request)).

-spec pcast(PoolName :: client_pool_name(), Request :: any()) ->
                   ok | {error, Reason :: any()}.
pcast(PoolName, Request) ->
    cast_pool(PoolName, term_to_binary(Request)).

-endif.
