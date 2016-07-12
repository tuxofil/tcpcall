%%% @doc
%%% Accepts new TCP connections

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 10 Nov 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall_acceptor).

-behaviour(gen_server).

%% API exports
-export(
   [start_link/1,
    clients/1,
    register_client/1,
    set_active/2,
    stop/1
   ]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("tcpcall.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

-define(SIG_ACCEPT, accept).
-define(SIG_SET_ACTIVE(IsActive), {set_active, IsActive}).

-record(state,
        {bind_port :: inet:port_number(),
         socket :: port(),
         receiver :: tcpcall:receiver(),
         max_parallel_requests :: pos_integer()
        }).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start TCP connection acceptor process (server) as part
%% of the supervision tree.
-spec start_link(Options :: tcpcall:listen_options()) ->
                        {ok, Pid :: pid()} | ignore |
                        {error, Reason :: any()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, _GenServerOptions = []).

%% @doc Return list of process IDs for active client connections.
-spec clients(Acceptor :: pid() | atom()) -> [pid()].
clients(Acceptor) ->
    case get_registry_pid(Acceptor) of
        {ok, RegistryPID} ->
            case process_info(RegistryPID, dictionary) of
                {dictionary, List} ->
                    [PID || {PID, _} <- List];
                undefined ->
                    []
            end;
        undefined ->
            []
    end.

%% @doc Called by just created client connection process to register
%% itself in the acceptor's registry.
%% Called from tcpcall_server:start/1 function.
-spec register_client(ClientConnectionPID :: pid()) ->
                             ok.
register_client(ClientConnectionPID) ->
    case get_registry_pid(self()) of
        {ok, RegistryPID} ->
            _Sent = RegistryPID ! {register, ClientConnectionPID},
            ok;
        undefined ->
            ok
    end.

%% @doc Tell the acceptor process to enable/disable server.
-spec set_active(BridgeRef :: tcpcall:bridge_ref(),
                 IsActive :: boolean()) -> ok.
set_active(BridgeRef, IsActive) when is_boolean(IsActive) ->
    ok = gen_server:call(BridgeRef, ?SIG_SET_ACTIVE(IsActive), infinity).

%% @doc Tell the acceptor process to stop.
-spec stop(BridgeRef :: tcpcall:bridge_ref()) -> ok.
stop(BridgeRef) ->
    _Sent = BridgeRef ! ?SIG_STOP,
    ok.

%% --------------------------------------------------------------------
%% gen_server callback functions
%% --------------------------------------------------------------------

%% @hidden
-spec init(tcpcall:listen_options()) -> {ok, InitialState :: #state{}}.
init(Options) ->
    undefined = put(registry, spawn_link(fun registry_loop/0)),
    case lists:keyfind(name, 1, Options) of
        {name, RegisteredName} ->
            true = register(RegisteredName, self());
        false ->
            ok
    end,
    {bind_port, BindPort} = lists:keyfind(bind_port, 1, Options),
    {receiver, Receiver} = lists:keyfind(receiver, 1, Options),
    MPR = proplists:get_value(max_parallel_requests, Options, 10000),
    true = 0 < MPR,
    InitialState =
        #state{
           bind_port = BindPort,
           receiver = Receiver,
           max_parallel_requests = MPR},
    {ok, listen(InitialState)}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), State :: #state{}}.
handle_info(?SIG_ACCEPT, State) when State#state.socket /= undefined ->
    case gen_tcp:accept(State#state.socket, 100) of
        {ok, Socket} ->
            ok = tcpcall_server:start(
                   [{socket, Socket},
                    {acceptor, self()},
                    {receiver, State#state.receiver},
                    {max_parallel_requests, State#state.max_parallel_requests}
                   ]);
        {error, timeout} ->
            ok
    end,
    ok = schedule_accept(),
    {noreply, State};
handle_info(?SIG_ACCEPT, State) ->
    %% not in active state
    {noreply, State};
handle_info(?SIG_STOP, State) ->
    {stop, _Reason = normal, State};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(?SIG_SET_ACTIVE(IsActive), _From, State)
  when IsActive /= (State#state.socket /= undefined) ->
    %% we are about changing server state
    if IsActive ->
            %% enabling the server
            {reply, ok, listen(State)};
       true ->
            %% disabling the server
            ok = gen_tcp:close(State#state.socket),
            lists:foreach(
              fun(ActiveConnectionPID) ->
                      catch tcpcall_server:stop(ActiveConnectionPID)
              end, clients(self())),
            {reply, ok, State#state{socket = undefined}}
    end;
handle_call(?SIG_SET_ACTIVE(_IsActive), _From, State) ->
    %% we're already in requested state => ignore
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Start listening configured TCP port number and update process
%% state term with created socket.
-spec listen(#state{}) -> NewState :: #state{}.
listen(State) ->
    SocketOpts =
        [{active, false}, binary, {reuseaddr, true}, {packet, 4},
         {keepalive, true}],
    {ok, Socket} = gen_tcp:listen(State#state.bind_port, SocketOpts),
    ok = schedule_accept(),
    State#state{socket = Socket}.

%% @doc Schedule new TCP connection accept.
-spec schedule_accept() -> ok.
schedule_accept() ->
    _Sent = self() ! ?SIG_ACCEPT,
    ok.

%% ----------------------------------------------------------------------
%% Registry for alive client connections

%% @doc Get registry process ID for acceptor.
-spec get_registry_pid(Acceptor :: pid() | atom()) -> {ok, pid()} | undefined.
get_registry_pid(Acceptor) ->
    AcceptorPID =
        if is_atom(Acceptor) ->
                whereis(Acceptor);
           true ->
                Acceptor
        end,
    if is_pid(AcceptorPID) ->
            case process_info(AcceptorPID, dictionary) of
                {dictionary, AcceptorDict} ->
                    case lists:keyfind(registry, 1, AcceptorDict) of
                        {registry, RegistryPID} ->
                            {ok, RegistryPID};
                        false ->
                            undefined
                    end;
                undefined ->
                    undefined
            end;
       true ->
            undefined
    end.

%% @doc
-spec registry_loop() -> no_return().
registry_loop() ->
    receive
        {register, PID} ->
            MonitorRef = monitor(process, PID),
            undefined = put(PID, MonitorRef),
            registry_loop();
        {'DOWN', MonitorRef, process, PID, _Reason} ->
            MonitorRef = erase(PID),
            registry_loop();
        Other ->
            throw({unknown_message, Other})
    end.

%% ----------------------------------------------------------------------
%% Unit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

active_state_test_() ->
    {setup,
     _StartUp =
         fun() ->
                 {ok, _} =
                     start_link(
                       [{name, ?MODULE},
                        {bind_port, 5001},
                        {receiver, fun(_) -> <<>> end}])
         end,
     _CleanUp =
         fun(_) ->
                 ok = stop(?MODULE)
         end,
     {inorder,
      [?_assertMatch({ok, _}, gen_tcp:connect("127.1", 5001, [])),
       ?_assertMatch(ok, set_active(?MODULE, true)),
       ?_assertMatch({ok, _}, gen_tcp:connect("127.1", 5001, [])),
       ?_assertMatch(ok, set_active(?MODULE, false)),
       ?_assertMatch({error, econnrefused}, gen_tcp:connect("127.1", 5001, [])),
       ?_assertMatch(ok, set_active(?MODULE, true)),
       ?_assertMatch({ok, _}, gen_tcp:connect("127.1", 5001, []))
      ]}}.

-endif.
