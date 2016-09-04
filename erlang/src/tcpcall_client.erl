%%% @doc
%%% Handles a client side of TCP connection.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 12 Nov 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall_client).

-behaviour(gen_server).

%% API exports
-export(
   [start_link/1,
    queue_request/5,
    queue_cast/4,
    wait_reply/2,
    wait_cast_ack/1,
    is_connected/1,
    stop/1,
    status/1
   ]).

%% Used by timer:apply_interval/4
-export([reg_vacuum/1]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("tcpcall.hrl").
-include("tcpcall_proto.hrl").
-include("tcpcall_types.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

-record(
   state,
   {socket :: port() | undefined,
    options :: tcpcall:client_options(),
    max_parallel_requests :: tcpcall:max_parallel_requests(),
    max_parallel_requests_policy :: tcpcall:max_parallel_requests_policy(),
    seq_num = 0 :: seq_num(),
    registry :: registry(),
    lord :: pid() | undefined
   }).

-type deadline() :: Micros :: pos_integer().

-define(VACUUM_PERIOD, 60 * 1000). %% one minute

%% internal signals
-define(SIG_CONNECT, connect).

%% ----------------------------------------------------------------------
%% Erlang interface definitions

%% sent when a local Erlang process calls tcpcall:call/3
-define(QUEUE_REQUEST(From, RequestRef, DeadLine, Request),
        {queue_request, From, RequestRef, DeadLine, Request}).

%% message with reply to a local Erlang process awaiting for reply
%% from remote side server.
-define(ARRIVE_REPLY(RequestRef, Reply),
        {arrive_reply, RequestRef, Reply}).

%% message with error report to a local Erlang process awaiting for reply
%% from remote side server.
-define(ARRIVE_ERROR(RequestRef, EncodedReason),
        {arrive_error, RequestRef, EncodedReason}).

%% sent when a local Erlang process calls tcpcall:cast/2
-define(QUEUE_CAST(From, RequestRef, Request),
        {queue_cast, From, RequestRef, Request}).

%% message with cast acknowledge to a local Erlang process.
-define(CAST_ACK(RequestRef),
        {cast_ack, RequestRef}).

%% message with cast deny to a local Erlang process.
-define(CAST_ERROR(RequestRef, Reason),
        {cast_error, RequestRef, Reason}).

%% --------------------------------------------------------------------
%% other definitions
-define(CONNECTED_FLAG, connected).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start TCP connection process which will connect to the
%% remote bridge.
%% The process can be started as part of the supervision tree.
-spec start_link(Options :: tcpcall:client_options()) ->
                   {ok, Pid :: pid()} | ignore |
                   {error, Reason :: any()}.
start_link(Options) ->
    gen_server:start_link(
      ?MODULE, Options, _GenServerOptions = []).

%% @doc Enqueue a request for transferring to the remote side.
-spec queue_request(BridgeRef :: tcpcall:bridge_ref(),
                    From :: pid(),
                    RequestRef :: reference(),
                    Deadline :: deadline(),
                    Request :: tcpcall:data()) ->
                           ok | {error, overload | notalive}.
queue_request(BridgeRef, From, RequestRef, DeadLine, Request) ->
    case queue_len(BridgeRef) of
        {ok, QueueLen} when QueueLen >= 2000 ->
            {error, overload};
        {ok, _QueueLen} ->
            gen_server:cast(
              BridgeRef,
              ?QUEUE_REQUEST(From, RequestRef, DeadLine, Request));
        undefined ->
            {error, notalive}
    end.

%% @doc Enqueue an asynchronous request (without a response)
%% for transferring to the remote side.
-spec queue_cast(BridgeRef :: tcpcall:bridge_ref(),
                 From :: pid(),
                 RequestRef :: reference(),
                 Request :: tcpcall:data()) ->
                        ok | {error, overload | notalive}.
queue_cast(BridgeRef, From, RequestRef, Request) ->
    case queue_len(BridgeRef) of
        {ok, QueueLen} when QueueLen >= 2000 ->
            {error, overload};
        {ok, _QueueLen} ->
            _Sent = BridgeRef ! ?QUEUE_CAST(From, RequestRef, Request),
            ok;
        undefined ->
            {error, notalive}
    end.

%% @doc Waits for reply from the remote side server.
-spec wait_reply(RequestRef :: reference(),
                 Timeout :: pos_integer()) ->
                        {ok, Reply :: tcpcall:data()} |
                        {error, Reason :: any()}.
wait_reply(RequestRef, Timeout) ->
    receive
        ?ARRIVE_REPLY(RequestRef, Reply) ->
            {ok, Reply};
        ?ARRIVE_ERROR(RequestRef, EncodedReason)
          when is_binary(EncodedReason) ->
            {error, binary_to_term(EncodedReason)};
        ?ARRIVE_ERROR(RequestRef, Reason) ->
            {error, Reason}
    after Timeout ->
            {error, timeout}
    end.

%% @doc Waits for cast acknowledge from socket. 'ok' means
%% the cast was sent to the server side.
-spec wait_cast_ack(RequestRef :: reference()) ->
                           ok | {error, Reason :: any()}.
wait_cast_ack(RequestRef) ->
    receive
        ?CAST_ACK(RequestRef) ->
            ok;
        ?CAST_ERROR(RequestRef, Reason) ->
            {error, Reason}
    after 5000 ->
            {error, timeout}
    end.

%% @doc Return 'true' if connected to the server.
-spec is_connected(BridgeRef :: tcpcall:bridge_ref()) -> boolean().
is_connected(BridgeRef) ->
    %% It was done in such non-usual manner to not block this
    %% request if the process is busy by network transfers.
    Pid =
        if is_atom(BridgeRef) ->
                whereis(BridgeRef);
           true ->
                BridgeRef
        end,
    if is_pid(Pid) ->
            case process_info(Pid, dictionary) of
                {dictionary, List} ->
                    lists:keyfind(?CONNECTED_FLAG, 1, List) /= false;
                undefined ->
                    false
            end;
       true ->
            false
    end.

%% @doc Tell the acceptor process to stop.
-spec stop(BridgeRef :: tcpcall:bridge_ref()) -> ok.
stop(BridgeRef) ->
    _Sent = BridgeRef ! ?SIG_STOP,
    ok.

%% @doc Show detailed status of the process.
-spec status(BridgeRef :: tcpcall:bridge_ref()) -> list().
status(BridgeRef) ->
    gen_server:call(BridgeRef, ?SIG_STATUS).

%% --------------------------------------------------------------------
%% gen_server callback functions
%% --------------------------------------------------------------------

%% @hidden
-spec init(tcpcall:client_options()) ->
                  {ok, InitialState :: #state{}}.
init(Options) ->
    case lists:keyfind(name, 1, Options) of
        {name, RegisteredName} ->
            true = register(RegisteredName, self());
        false ->
            ok
    end,
    DefaultMPR =
        application:get_env(
          tcpcall, client_default_max_parallel_requests, 10000),
    MPR = proplists:get_value(max_parallel_requests, Options, DefaultMPR),
    DefaultMPRP =
        application:get_env(
          tcpcall, client_default_max_parallel_requests_policy, ?drop_old),
    MPRP = proplists:get_value(max_parallel_requests_policy, Options, DefaultMPRP),
    %% a mapping from SeqNum (of arrived reply from the
    %% socket) to RequestRef of the request sent by a
    %% local Erlang process
    Registry = reg_new(),
    %% schedule connect to the remote host immediately
    _Sent = self() ! ?SIG_CONNECT,
    {ok,
     #state{options = Options,
            max_parallel_requests = MPR,
            max_parallel_requests_policy = MPRP,
            registry = Registry,
            lord = proplists:get_value(lord, Options)}}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), NewState :: #state{}}.
handle_info(?QUEUE_CAST(From, RequestRef, Request), State)
  when State#state.socket /= undefined ->
    %% Received an asynchronous request from a local Erlang process
    SeqNum =
        if State#state.seq_num > ?MAX_SEQ_NUM ->
                0;
           true ->
                State#state.seq_num
        end,
    case gen_tcp:send(
           State#state.socket,
           ?PACKET_CAST(SeqNum, Request)) of
        ok ->
            _Sent = From ! ?CAST_ACK(RequestRef),
            {noreply, State#state{seq_num = SeqNum + 1}};
        {error, Reason} ->
            %% Failed to send. Reply to the local process
            %% immediately and try to reconnect.
            _Sent = From ! ?CAST_ERROR(RequestRef, Reason),
            {noreply, connect(State)}
    end;
handle_info(?QUEUE_CAST(From, RequestRef, _Request), State) ->
    %% not connected. Reply to the caller immediately.
    _Sent = From ! ?CAST_ERROR(RequestRef, not_connected),
    {noreply, State};
handle_info({tcp, Socket, Data}, State)
  when Socket == State#state.socket ->
    %% process data from the socket only when connected
    ok = handle_data_from_net(State, Data),
    {noreply, State};
handle_info(?SIG_CONNECT, State) ->
    {noreply, connect(State)};
handle_info(?SIG_STOP, State) ->
    ok = lord_report(State, false),
    {stop, normal, State};
handle_info({tcp_closed, Socket}, State)
  when Socket == State#state.socket ->
    ok = lord_report(State, false),
    %% try to reconnect immediately
    {noreply, connect(State)};
handle_info({tcp_error, Socket, _Reason}, State)
  when Socket == State#state.socket ->
    ok = lord_report(State, false),
    %% try to reconnect immediately
    {noreply, connect(State)};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(?QUEUE_REQUEST(From, RequestRef, Deadline, Request), State)
  when State#state.socket /= undefined ->
    %% Received a request from a local Erlang process
    SeqNum =
        if State#state.seq_num > ?MAX_SEQ_NUM ->
                0;
           true ->
                State#state.seq_num
        end,
    case register_request_from_local_process(
           State, RequestRef, From, SeqNum, Deadline) of
        ok ->
            case gen_tcp:send(
                   State#state.socket,
                   ?PACKET_REQUEST(SeqNum, Deadline, Request)) of
                ok ->
                    {noreply, State#state{seq_num = SeqNum + 1}};
                {error, Reason} ->
                    %% Failed to send. Reply to the local process
                    %% immediately and try to reconnect.
                    _Sent = From ! ?ARRIVE_ERROR(RequestRef, Reason),
                    reg_del(State#state.registry, SeqNum),
                    {noreply, connect(State)}
            end;
        overload ->
            %% Request registry is overloaded.
            %% Reply to the local process immediately.
            _Sent = From ! ?ARRIVE_ERROR(RequestRef, overload),
            {noreply, State}
    end;
handle_cast(?QUEUE_REQUEST(From, RequestRef, _DeadLine, _Request), State) ->
    %% not connected. Reply to the caller immediately.
    _Sent = From ! ?ARRIVE_ERROR(RequestRef, not_connected),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {reply, any(), NewState :: #state{}} |
                         {noreply, NewState :: #state{}}.
handle_call(?SIG_STATUS, _From, State) ->
    {reply,
     [{socket, State#state.socket},
      {peer,
       try
           {ok, Peer} = inet:peername(State#state.socket),
           Peer
       catch _:_ ->
               undefined
       end},
      {sync_requests, reg_size(State#state.registry)},
      {max_parallel_requests,
       State#state.max_parallel_requests,
       get_max_parallel_requests(State)},
      {max_parallel_requests_policy,
       State#state.max_parallel_requests_policy,
       get_max_parallel_requests_policy(State)},
      {options, State#state.options}
     ],
     State};
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

%% @doc Connect to the remote side.
-spec connect(State :: #state{}) -> NewState :: #state{}.
connect(State) ->
    %% disconnect if needed
    if State#state.socket /= undefined ->
            catch gen_tcp:close(State#state.socket),
            ok;
       true ->
            ok
    end,
    _OldConnectedFlag = erase(?CONNECTED_FLAG),
    %% the function is called only in 'client' mode, so
    %% clear the registry
    ok = reg_clear(State#state.registry),
    {host, Host} = lists:keyfind(host, 1, State#state.options),
    {port, Port} = lists:keyfind(port, 1, State#state.options),
    ConnTimeout =
        proplists:get_value(conn_timeout, State#state.options, 2000),
    SocketOptions =
        [binary, {packet, 4}, {active, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, SocketOptions, ConnTimeout) of
        {ok, Socket} ->
            undefined = put(?CONNECTED_FLAG, true),
            ok = lord_report(State, true),
            State#state{socket = Socket,
                        seq_num = 0};
        {error, _Reason} ->
            %% schedule reconnect after some timeout
            {ok, _TRef} = timer:send_after(500, ?SIG_CONNECT),
            State#state{socket = undefined,
                        seq_num = 0}
    end.

%% @doc Register request from a local Erlang process.
-spec register_request_from_local_process(
        #state{},
        RequestRef :: reference(),
        From :: pid(),
        seq_num(),
        deadline()) -> ok | overload.
register_request_from_local_process(State, RequestRef, From, SeqNum, Deadline) ->
    Registry = State#state.registry,
    MaxParallelRequests = get_max_parallel_requests(State),
    case get_max_parallel_requests_policy(State) of
        ?deny_new ->
            %% If request registry is full, do not register new request
            %% and reply immediately with 'overload'
            case reg_size(Registry) of
                RecordsCount when MaxParallelRequests =< RecordsCount ->
                    case reg_vacuum(State#state.registry) of
                        0 ->
                            overload;
                        _PosInteger ->
                            reg_add(Registry, SeqNum, From, RequestRef, Deadline)
                    end;
                _RecordsCount ->
                    reg_add(Registry, SeqNum, From, RequestRef, Deadline)
            end;
        ?drop_old ->
            %% If request registry is full, drop the eldest record from it.
            reg_add(Registry, SeqNum, From, RequestRef, Deadline),
            case reg_size(Registry) of
                RecordsCount when MaxParallelRequests < RecordsCount ->
                    reg_del_eldest(Registry);
                _RecordsCount ->
                    ok
            end
    end.

%% @doc Handle a data packet received from the network socket.
-spec handle_data_from_net(State :: #state{}, Data :: binary()) -> ok.
handle_data_from_net(State, ?PACKET_REPLY(SeqNum, Reply)) ->
    case reg_fetch(State#state.registry, SeqNum) of
        {ok, RequestRef, From} ->
            _Sent = From ! ?ARRIVE_REPLY(RequestRef, Reply),
            ok;
        undefined ->
            %% ignore
            ok
    end;
handle_data_from_net(State, ?PACKET_ERROR(SeqNum, EncodedReason)) ->
    case reg_fetch(State#state.registry, SeqNum) of
        {ok, RequestRef, From} ->
            _Sent = From ! ?ARRIVE_ERROR(RequestRef, EncodedReason),
            ok;
        undefined ->
            %% ignore
            ok
    end;
handle_data_from_net(State, ?PACKET_FLOW_CONTROL_SUSPEND(Millis)) ->
    ok = do_suspend_hook(State, Millis);
handle_data_from_net(State, ?PACKET_FLOW_CONTROL_RESUME) ->
    case lists:keyfind(resume_handler, 1, State#state.options) of
        {resume_handler, undefined} ->
            ok;
        {resume_handler, PID} when is_atom(PID) orelse is_pid(PID) ->
            _Sent = PID ! {tcpcall_resume, self()},
            ok;
        {resume_handler, Fun} when is_function(Fun, 0) ->
            _Ignored = Fun(),
            ok;
        false ->
            ok
    end;
handle_data_from_net(State, ?PACKET_UPLINK_CAST(Data)) ->
    case lists:keyfind(uplink_cast_handler, 1, State#state.options) of
        {uplink_cast_handler, undefined} ->
            ok;
        {uplink_cast_handler, PID} when is_atom(PID) orelse is_pid(PID) ->
            _Sent = PID ! {tcpcall_uplink_cast, self(), Data},
            ok;
        {uplink_cast_handler, Fun} when is_function(Fun, 1) ->
            _Ignored = Fun(Data),
            ok;
        false ->
            ok
    end;
handle_data_from_net(_State, _BadOrUnknownPacket) ->
    %% ignore
    ok.

%% @doc Return current message queue len for a process.
-spec queue_len(atom() | pid()) -> {ok, non_neg_integer()} | undefined.
queue_len(Atom) when is_atom(Atom) ->
    case whereis(Atom) of
        undefined ->
            undefined;
        Pid ->
            queue_len(Pid)
    end;
queue_len(Pid) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Len} ->
            {ok, Len};
        undefined ->
            undefined
    end.

%% @doc Send notification to master process with
%% current connection state.
-spec lord_report(#state{}, IsConnected :: boolean()) -> ok.
lord_report(State, IsConnected) when is_pid(State#state.lord) ->
    _Sent = State#state.lord ! {?MODULE, self(), IsConnected},
    ok;
lord_report(_State, _IsConnected) ->
    ok.

%% @doc Fire hook for suspend event.
-spec do_suspend_hook(#state{}, Millis :: non_neg_integer()) -> ok.
do_suspend_hook(State, Millis) ->
    case lists:keyfind(suspend_handler, 1, State#state.options) of
        {suspend_handler, undefined} ->
            ok;
        {suspend_handler, PID} when is_atom(PID) orelse is_pid(PID) ->
            catch PID ! {tcpcall_suspend, self(), Millis},
            ok;
        {suspend_handler, Fun} when is_function(Fun, 1) ->
            _Ignored = Fun(Millis),
            ok;
        false ->
            ok
    end.

%% @doc Get value for max_parallel_requests option.
-spec get_max_parallel_requests(#state{}) -> pos_integer().
get_max_parallel_requests(#state{max_parallel_requests = MPR})
  when is_integer(MPR) ->
    MPR;
get_max_parallel_requests(#state{max_parallel_requests = MPR})
  when is_function(MPR, 0) ->
    MPR().

%% @doc Get value for max_parallel_requests_policy option.
-spec get_max_parallel_requests_policy(#state{}) -> ?drop_old | ?deny_new.
get_max_parallel_requests_policy(#state{max_parallel_requests_policy = MPRP})
  when is_atom(MPRP) ->
    MPRP;
get_max_parallel_requests_policy(#state{max_parallel_requests_policy = MPRP})
  when is_function(MPRP, 0) ->
    MPRP().

%% ----------------------------------------------------------------------
%% Registry management functions

-record(
   rrec,
   {seq_num :: seq_num(),
    caller :: pid(),
    req_id :: reference(),
    deadline :: deadline()
   }).

%% @doc Create new registry table.
-spec reg_new() -> registry().
reg_new() ->
    %% The table is public to allow vacuuming from the
    %% another process.
    Registry = ets:new(?MODULE, [ordered_set, public, {keypos, 2}]),
    %% Schedule periodic vacuuming.
    {ok, _TRef} =
        timer:apply_interval(
          ?VACUUM_PERIOD, ?MODULE, reg_vacuum, [Registry]),
    Registry.

%% @doc Add new entry to the registry.
-spec reg_add(registry(),
              seq_num(),
              Caller :: pid(),
              RequestRef :: reference(),
              deadline()) -> ok.
reg_add(Registry, SeqNum, Caller, RequestRef, Deadline) ->
    true = ets:insert(Registry, #rrec{seq_num = SeqNum,
                                      caller = Caller,
                                      req_id = RequestRef,
                                      deadline = Deadline}),
    ok.

%% @doc Remove entry from the registry.
-spec reg_del(registry(), seq_num()) -> ok.
reg_del(Registry, SeqNum) ->
    true = ets:delete(Registry, SeqNum),
    ok.

%% @doc Remove the eldest record from the registry,
%% and send 'timeout' error to the caller process
-spec reg_del_eldest(registry()) -> ok.
reg_del_eldest(Registry) ->
    case ets:first(Registry) of
        '$end_of_table' ->
            ok;
        SeqNum ->
            case ets:lookup(Registry, SeqNum) of
                [#rrec{caller = Caller, req_id = RequestRef}] ->
                    _Sent = Caller ! ?ARRIVE_ERROR(RequestRef, timeout),
                    true = ets:delete(Registry, SeqNum),
                    ok;
                [] ->
                    ok
            end
    end.

%% @doc Return total count of records in the registry.
-spec reg_size(registry()) -> non_neg_integer().
reg_size(Registry) ->
    ets:info(Registry, size).

%% @doc Lookup RequestRef by the SeqNum and remove the record.
-spec reg_fetch(registry(), seq_num()) ->
                       {ok,
                        RequestRef :: reference(),
                        Caller :: pid()} |
                       undefined.
reg_fetch(Registry, SeqNum) ->
    case ets:lookup(Registry, SeqNum) of
        [#rrec{caller = Caller, req_id = RequestRef}] ->
            true = ets:delete(Registry, SeqNum),
            {ok, RequestRef, Caller};
        [] ->
            undefined
    end.

%% @doc Clear the clients registry.
%% All records will be deleted and all pending requests will
%% be discarded, sending the answers like {error, disconnected}.
-spec reg_clear(registry()) -> ok.
reg_clear(Registry) ->
    Reason = disconnected,
    undefined =
        ets:foldl(
          fun(#rrec{caller = Caller, req_id = RequestRef}, Accum) ->
                  _Sent = Caller ! ?ARRIVE_ERROR(RequestRef, Reason),
                  Accum
          end, undefined, Registry),
    true = ets:delete_all_objects(Registry),
    ok.

%% @hidden
%% @doc Remove all expired items from the registry.
-spec reg_vacuum(registry()) -> Removed :: non_neg_integer().
reg_vacuum(Registry) ->
    Now = tcpcall_lib:micros(),
    try
        ets:foldl(
          fun(#rrec{seq_num = SeqNum,
                    caller = Caller,
                    req_id = RequestRef,
                    deadline = Deadline}, Accum)
                when Deadline < Now ->
                  true = ets:delete(Registry, SeqNum),
                  %% notify the caller
                  _Sent = Caller ! ?ARRIVE_ERROR(RequestRef, timeout),
                  Accum + 1;
             (_NotExpiredEntry, Accum) ->
                  throw({break, Accum})
          end, 0, Registry)
    catch
        throw:{break, Removed} ->
            Removed
    end.
