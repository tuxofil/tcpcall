%%% @doc
%%% Handles a TCP connection.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 10 Nov 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall_server).

-behaviour(gen_server).

%% API exports
-export(
   [start/1,
    queue_reply/3,
    suspend/2,
    resume/1,
    uplink_cast/2,
    stop/1
   ]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

%% Used by timer:apply_interval/4
-export([vacuum/1]).

-include("tcpcall.hrl").
-include("tcpcall_proto.hrl").
-include("tcpcall_types.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

-export_type(
   [server_options/0,
    server_option/0
   ]).

-type server_options() :: [server_option()].

-type server_option() ::
        {socket, port()} |
        {acceptor, pid()} |
        {receiver, tcpcall:receiver()} |
        {max_parallel_requests, tcpcall:max_parallel_requests()} |
        {overflow_suspend_period, tcpcall:overflow_suspend_period()} |
        {max_message_queue_len, tcpcall:max_message_queue_len()} |
        {queue_overflow_suspend_period, tcpcall:queue_overflow_suspend_period()}.

-record(
   state,
   {socket :: port(),
    options :: server_options(),
    max_parallel_requests :: tcpcall:max_parallel_requests(),
    overflow_suspend_period :: tcpcall:overflow_suspend_period(),
    max_message_queue_len :: tcpcall:max_message_queue_len(),
    queue_overflow_suspend_period :: tcpcall:queue_overflow_suspend_period(),
    ready = false :: boolean(),
    acceptor_pid :: pid(),
    acceptor_mon :: reference(),
    receiver :: tcpcall:receiver(),
    registry :: registry()
   }).

-define(VACUUM_PERIOD, 60 * 1000). %% one minute

%% internal signals
-define(SIG_READY, ready).
-define(SIG_SELF_DESTRUCT, self_destruct).

%% gauge for spawned workers for cast requests
-define(async_workers, async_workers).

%% ----------------------------------------------------------------------
%% Erlang interface definitions

%% message with request to a local receiver process (on the server side)
-define(ARRIVE_REQUEST(BridgeRef, RequestRef, Request),
        {tcpcall_req, BridgeRef, RequestRef, Request}).

%% message with asynchronous request (without a response) to a local
%% receiver process (on the server side)
-define(ARRIVE_CAST(BridgeRef, Request),
        {tcpcall_cast, BridgeRef, Request}).

%% sent when the receiver process prepare a reply
-define(QUEUE_REPLY(RequestRef, Reply),
        {queue_reply, RequestRef, Reply}).

%% sent when the receiver process is unable to process the request
-define(QUEUE_ERROR(RequestRef, Reason),
        {queue_error, RequestRef, Reason}).

%% sent to the server to ask all connected clients to stop sending
%% new data for a while
-define(SUSPEND(Millis),
        {suspend, Millis}).

%% sent to the server to ask all connected clients to disable suspend mode
-define(RESUME, resume).

%% signal to server to send some data to the client side
-define(QUEUE_UPLINK_CAST(Data),
        {uplink_cast, Data}).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start TCP connection process (server side).
%% The function is called from tcpcall_acceptor module.
%% The process is spawned unlinked.
-spec start(Options :: server_options()) -> ok.
start(Options) ->
    {ok, Pid} =
        gen_server:start(
          ?MODULE, Options, _GenServerOptions = []),
    {socket, Socket} = lists:keyfind(socket, 1, Options),
    case gen_tcp:controlling_process(Socket, Pid) of
        ok ->
            ok = gen_server:cast(Pid, ?SIG_READY),
            ok = tcpcall_acceptor:register_client(Pid);
        {error, closed} ->
            %% the server is going down
            ok
    end.

%% @doc Enqueue a reply for transferring to the remote side.
-spec queue_reply(BridgeRef :: tcpcall:bridge_ref(),
                  RequestRef :: reference(),
                  Reply :: tcpcall:data()) -> ok.
queue_reply(BridgeRef, RequestRef, Reply) ->
    ok = gen_server:cast(BridgeRef, ?QUEUE_REPLY(RequestRef, Reply)).

%% @doc Ask all connected clients to not sent new data for a few time.
%% Usually called from the request processor to ask for load decrease.
-spec suspend(BridgeRef :: tcpcall:bridge_ref(),
              Millis :: non_neg_integer()) -> ok.
suspend(BridgeRef, Millis) when is_integer(Millis), 0 =< Millis ->
    ok = gen_server:cast(BridgeRef, ?SUSPEND(Millis)).

%% @doc Ask all connected clients to disable suspend mode and continue
%% to send new data. Usually called from the request processor.
-spec resume(BridgeRef :: tcpcall:bridge_ref()) -> ok.
resume(BridgeRef) ->
    ok = gen_server:cast(BridgeRef, ?RESUME).

%% @doc Send responseless cast to the client side.
-spec uplink_cast(BridgeRef :: tcpcall:bridge_ref(), Data :: binary()) -> ok.
uplink_cast(BridgeRef, Data) when is_binary(Data) ->
    ok = gen_server:cast(BridgeRef, ?QUEUE_UPLINK_CAST(Data)).

%% @hidden
%% @doc Enqueue an error reply for transferring to the remote side.
%% The function is not a part of module public API.
-spec queue_error(BridgeRef :: tcpcall:bridge_ref(),
                  RequestRef :: reference(),
                  Reason :: any()) -> ok.
queue_error(BridgeRef, RequestRef, Reason) ->
    EncodedReason = term_to_binary(Reason),
    ok = gen_server:cast(
           BridgeRef, ?QUEUE_ERROR(RequestRef, EncodedReason)).

%% @doc Stop process, closing connection to the client.
-spec stop(BridgeRef :: tcpcall:bridge_ref()) -> ok.
stop(BridgeRef) ->
    ok = gen_server:call(BridgeRef, ?SIG_STOP).

%% --------------------------------------------------------------------
%% gen_server callback functions
%% --------------------------------------------------------------------

%% @hidden
-spec init(server_options()) ->
                  {ok, InitialState :: #state{}}.
init(Options) ->
    %% a mapping from RequestRef (of arrived request from
    %% the socket) to SeqNum for the reply which is going
    %% to send to the client side.
    %% The table is public to allow vacuuming from the
    %% another process.
    Registry = ets:new(?MODULE, [public]),
    %% If the 'self_destruct' signal will arrive before the 'ready'
    %% signal, the process will terminate.
    {ok, _TRef} = timer:send_after(1000, ?SIG_SELF_DESTRUCT),
    %% Monitor acceptor process. When it terminate, we will terminate too
    {acceptor, AcceptorPid} = lists:keyfind(acceptor, 1, Options),
    MonitorRef = monitor(process, AcceptorPid),
    {socket, Socket} = lists:keyfind(socket, 1, Options),
    {receiver, Receiver} = lists:keyfind(receiver, 1, Options),
    MPR = proplists:get_value(max_parallel_requests, Options),
    OSP = proplists:get_value(overflow_suspend_period, Options),
    MMQL = proplists:get_value(max_message_queue_len, Options),
    QOSP = proplists:get_value(queue_overflow_suspend_period, Options),
    %% initialize gauge for spawned cast workers
    undefined = put(?async_workers, 0),
    {ok,
     #state{socket = Socket,
            ready = false, %% will wait for 'ready' signal
            options = Options,
            max_parallel_requests = MPR,
            overflow_suspend_period = OSP,
            max_message_queue_len = MMQL,
            queue_overflow_suspend_period = QOSP,
            acceptor_pid = AcceptorPid,
            acceptor_mon = MonitorRef,
            receiver = Receiver,
            registry = Registry}}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), NewState :: #state{}}.
handle_info({tcp, Socket, Data}, State)
  when Socket == State#state.socket, State#state.ready ->
    %% process data from the socket only when up and ready
    case check_message_queue_len(State) of
        ok ->
            case handle_data_from_net(State, Data) of
                ok ->
                    {noreply, State};
                stop ->
                    {stop, normal, State}
            end;
        stop ->
            {stop, normal, State}
    end;
handle_info(?SIG_SELF_DESTRUCT, State) when not State#state.ready ->
    %% The 'self_destruct' signal arrived before the
    %% 'ready' signal. Something went wrong, cannot continue.
    {stop, normal, State};
handle_info({tcp_closed, Socket}, State)
  when Socket == State#state.socket ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State)
  when Socket == State#state.socket ->
    {stop, normal, State};
handle_info({'DOWN', MonitorRef, process, AcceptorPid, _Reason}, State)
  when MonitorRef == State#state.acceptor_mon,
       AcceptorPid == State#state.acceptor_pid ->
    %% connection acceptor process is down.
    {stop, normal, State};
handle_info({'DOWN', _MonRef, process, _CastWorkerPid, _Reason}, State) ->
    %% only spawned workers for cast requests are being monitored
    %% except Acceptor process. Decrement gauge.
    _OldValue = put(?async_workers, get(?async_workers) - 1),
    {noreply, State};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}} |
                         {stop, Reason :: any(), NewState :: #state{}}.
handle_cast(?QUEUE_REPLY(RequestRef, Reply), State) ->
    %% Received a valid reply from the receiver process
    case pop_seq_num(State#state.registry, RequestRef) of
        {ok, SeqNum} ->
            case gen_tcp:send(
                   State#state.socket,
                   ?PACKET_REPLY(SeqNum, Reply)) of
                ok ->
                    {noreply, State};
                {error, _Reason} ->
                    {stop, normal, State}
            end;
        undefined ->
            {noreply, State}
    end;
handle_cast(?QUEUE_ERROR(RequestRef, Reason), State) ->
    %% Received an error message from the receiver process
    case pop_seq_num(State#state.registry, RequestRef) of
        {ok, SeqNum} ->
            case gen_tcp:send(
                   State#state.socket,
                   ?PACKET_ERROR(SeqNum, Reason)) of
                ok ->
                    {noreply, State};
                {error, _Reason} ->
                    {stop, normal, State}
            end;
        undefined ->
            {noreply, State}
    end;
handle_cast(?SIG_READY, State) ->
    %% The signal is sent by the acceptor process when it
    %% transfers socket ownership to the handler process.
    %% From the moment we can use the socket.
    ok = inet:setopts(State#state.socket, [{active, true}]),
    %% Schedule periodic vacuuming.
    {ok, _TRef} =
        timer:apply_interval(
          ?VACUUM_PERIOD,
          ?MODULE, vacuum, [State#state.registry]),
    {noreply, State#state{ready = true}};
handle_cast(?SUSPEND(Millis), State) ->
    case gen_tcp:send(
           State#state.socket,
           ?PACKET_FLOW_CONTROL_SUSPEND(Millis)) of
        ok ->
            {noreply, State};
        {error, _Reason} ->
            {stop, normal, State}
    end;
handle_cast(?RESUME, State) ->
    case gen_tcp:send(
           State#state.socket, ?PACKET_FLOW_CONTROL_RESUME) of
        ok ->
            {noreply, State};
        {error, _Reason} ->
            {stop, normal, State}
    end;
handle_cast(?QUEUE_UPLINK_CAST(Data), State) ->
    case gen_tcp:send(
           State#state.socket, ?PACKET_UPLINK_CAST(Data)) of
        ok ->
            {noreply, State};
        {error, _Reason} ->
            {stop, normal, State}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {stop, Reason :: normal, Reply :: ok, #state{}} |
                         {noreply, NewState :: #state{}}.
handle_call(?SIG_STOP, _From, State) ->
    {stop, _Reason = normal, _Reply = ok, State};
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

%% @doc Handle a data packet from arrived from the network socket.
-spec handle_data_from_net(State :: #state{}, Data :: binary()) ->
                                  ok | stop.
handle_data_from_net(State, ?PACKET_REQUEST(SeqNum, DeadLine, Request)) ->
    RequestRef = make_ref(),
    case register_request_from_network(State, SeqNum, RequestRef, DeadLine) of
        ok ->
            %% relay the request to the receiver process
            case deliver_request(State#state.receiver, RequestRef, Request) of
                ok ->
                    ok;
                error ->
                    %% immediately reply to the remote side with error
                    Reply = term_to_binary(no_proc),
                    case gen_tcp:send(
                           State#state.socket,
                           ?PACKET_ERROR(SeqNum, Reply)) of
                        ok ->
                            ok;
                        {error, _Reason} ->
                            %% connection is broken. Terminate.
                            stop
                    end
            end;
        overload ->
            %% immediately reply to the remote side with error
            Reply = term_to_binary(overload),
            case gen_tcp:send(
                   State#state.socket,
                   ?PACKET_ERROR(SeqNum, Reply)) of
                ok ->
                    ok;
                {error, _Reason} ->
                    %% connection is broken. Terminate.
                    stop
            end
    end;
handle_data_from_net(State, ?PACKET_CAST(_SeqNum, Request)) ->
    %% relay the cast to the receiver process
    ok = deliver_cast(State#state.receiver, Request),
    OSP = State#state.overflow_suspend_period,
    is_overloaded(State) andalso
        suspend(
          self(),
          if is_integer(OSP) ->
                  OSP;
             is_function(OSP, 0) ->
                  OSP()
          end),
    ok;
handle_data_from_net(_State, _BadOrUnknownPacket) ->
    %% ignore
    ok.

%% @doc Register request arrived from the network.
-spec register_request_from_network(
        #state{},
        SeqNum :: seq_num(),
        RequestRef :: reference(),
        DeadLine :: pos_integer()) -> ok | overload.
register_request_from_network(State, SeqNum, RequestRef, DeadLine) ->
    case is_overloaded(State) of
        true ->
            overload;
        false ->
            true = ets:insert(State#state.registry, {RequestRef, SeqNum, DeadLine}),
            ok
    end.

%% @doc Deliver request received from the remote side (the client)
%% to the local receiver process.
-spec deliver_request(Receiver :: tcpcall:receiver(),
                      RequestRef :: reference(),
                      Request :: tcpcall:data()) ->
                             ok | error.
deliver_request(ReceiverName, RequestRef, Request)
  when is_atom(ReceiverName) ->
    case whereis(ReceiverName) of
        Pid when is_pid(Pid) ->
            deliver_request(Pid, RequestRef, Request);
        undefined ->
            error
    end;
deliver_request(Pid, RequestRef, Request) when is_pid(Pid) ->
    ServerPid = self(),
    case is_process_alive(Pid) of
        true ->
            Msg = ?ARRIVE_REQUEST(ServerPid, RequestRef, Request),
            _Sent = Pid ! Msg,
            ok;
        false ->
            error
    end;
deliver_request(FunObject, RequestRef, Request)
  when is_function(FunObject, 1) ->
    ServerPid = self(),
    _Pid =
        spawn_link(
          fun() ->
                  try FunObject(Request) of
                      Reply when is_binary(Reply) ->
                          queue_reply(
                            ServerPid, RequestRef, Reply)
                  catch
                      ExcType:ExcReason ->
                          queue_error(
                            ServerPid, RequestRef,
                            {crashed,
                             [{type, ExcType},
                              {reason, ExcReason},
                              {stacktrace,
                               erlang:get_stacktrace()}]})
                  end
          end),
    ok.

%% @doc Deliver cast (asynchronous request without a response) received
%% from the remote side (the client) to the local receiver process.
-spec deliver_cast(Receiver :: tcpcall:receiver(), Request :: tcpcall:data()) -> ok.
deliver_cast(ReceiverName, Request)
  when is_atom(ReceiverName) ->
    case whereis(ReceiverName) of
        Pid when is_pid(Pid) ->
            deliver_cast(Pid, Request);
        undefined ->
            ok
    end;
deliver_cast(Pid, Request) when is_pid(Pid) ->
    ServerPid = self(),
    case is_process_alive(Pid) of
        true ->
            _Sent = Pid ! ?ARRIVE_CAST(ServerPid, Request),
            ok;
        false ->
            ok
    end;
deliver_cast(FunObject, Request)
  when is_function(FunObject, 1) ->
    Pid =
        spawn_link(
          fun() ->
                  _Ignored = (catch FunObject(Request)),
                  ok
          end),
    _MonRef = monitor(process, Pid),
    _OldValue = put(?async_workers, get(?async_workers) + 1),
    ok.

%% @doc Lookup SeqNum by RequestRef and remove it from the
%% registry.
-spec pop_seq_num(Registry :: registry(),
                  RequestRef :: reference()) ->
                         {ok, SeqNum :: seq_num()} |
                         undefined.
pop_seq_num(Registry, RequestRef) ->
    case ets:lookup(Registry, RequestRef) of
        [{RequestRef, SeqNum, DeadLine}] ->
            true = ets:delete(Registry, RequestRef),
            Now = tcpcall_lib:micros(),
            if Now >= DeadLine ->
                    %% outdated reply. ignore it
                    undefined;
               true ->
                    {ok, SeqNum}
            end;
        [] ->
            undefined
    end.

%% @hidden
%% @doc Remove all expired items from the registry.
-spec vacuum(Registry :: registry()) -> ok.
vacuum(Registry) ->
    Now = tcpcall_lib:micros(),
    undefined =
        ets:foldl(
          fun({RequestRef, _SeqNum, DeadLine}, Accum)
             when Now >= DeadLine ->
                  true = ets:delete(Registry, RequestRef),
                  Accum;
             (_, Accum) ->
                  Accum
          end, undefined, Registry),
    ok.

%% @doc Return 'true' when configured max count of worker processes
%% is less than count of actually running workers.
-spec is_overloaded(#state{}) -> boolean().
is_overloaded(State) ->
    MPR = State#state.max_parallel_requests,
    if is_integer(MPR) ->
            MPR =< workers_count(State);
       is_function(MPR, 0) ->
            MPR() =< workers_count(State)
    end.

%% @doc Return total count of running workers. This include
%% workers for sync requests and workers for casts (async requests).
-spec workers_count(#state{}) -> non_neg_integer().
workers_count(State) ->
    RegisteredSyncRequests = ets:info(State#state.registry, size),
    SpawnedAsyncRequests = get(?async_workers),
    RegisteredSyncRequests + SpawnedAsyncRequests.

%% @doc Check the size of process message queue.
-spec check_message_queue_len(#state{}) -> ok | stop.
check_message_queue_len(State) ->
    MMQL = State#state.max_message_queue_len,
    MaxMessageQueueLen =
        if is_integer(MMQL) ->
                MMQL;
           is_function(MMQL, 0) ->
                MMQL()
        end,
    case process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len < MaxMessageQueueLen ->
            %% message queue length is of normal size
            ok;
        _Overload ->
            %% ask clients for suspend
            QOSP = State#state.queue_overflow_suspend_period,
            Millis =
                if is_integer(QOSP) ->
                        QOSP;
                   is_function(QOSP) ->
                        QOSP()
                end,
            case gen_tcp:send(
                   State#state.socket,
                   ?PACKET_FLOW_CONTROL_SUSPEND(Millis)) of
                ok ->
                    ok;
                {error, _Reason} ->
                    stop
            end
    end.
