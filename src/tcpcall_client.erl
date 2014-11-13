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
    wait_reply/2,
    stop/1
   ]).

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
    seq_num = 0 :: seq_num(),
    registry :: registry()
   }).

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
                    DeadLine :: pos_integer(),
                    Request :: tcpcall:data()) -> ok.
queue_request(BridgeRef, From, RequestRef, DeadLine, Request) ->
    case queue_len(BridgeRef) >= 2000 of
        true ->
            %% the client process message queue is overloaded
            _Sent = self() ! ?ARRIVE_ERROR(RequestRef, overload),
            ok;
        false ->
            ok = gen_server:cast(
                   BridgeRef,
                   ?QUEUE_REQUEST(From, RequestRef, DeadLine, Request))
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

%% @doc Tell the acceptor process to stop.
-spec stop(BridgeRef :: tcpcall:bridge_ref()) -> ok.
stop(BridgeRef) ->
    _Sent = BridgeRef ! ?SIG_STOP,
    ok.

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
    %% a mapping from SeqNum (of arrived reply from the
    %% socket) to RequestRef of the request sent by a
    %% local Erlang process
    Registry = ets:new(?MODULE, []),
    %% schedule connect to the remote host immediately
    _Sent = self() ! ?SIG_CONNECT,
    {ok,
     #state{options = Options,
            registry = Registry}}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), NewState :: #state{}}.
handle_info({tcp, Socket, Data}, State)
  when Socket == State#state.socket ->
    %% process data from the socket only when connected
    ok = handle_data_from_net(State, Data),
    {noreply, State};
handle_info(?SIG_CONNECT, State) ->
    {noreply, connect(State)};
handle_info(?SIG_STOP, State) ->
    {stop, _Reason = normal, State};
handle_info({tcp_closed, Socket}, State)
  when Socket == State#state.socket ->
    %% try to reconnect immediately
    {noreply, connect(State)};
handle_info({tcp_error, Socket, _Reason}, State)
  when Socket == State#state.socket ->
    %% try to reconnect immediately
    {noreply, connect(State)};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(?QUEUE_REQUEST(From, RequestRef, DeadLine, Request), State)
  when State#state.socket /= undefined ->
    %% Received a request from a local Erlang process
    SeqNum =
        if State#state.seq_num > ?MAX_SEQ_NUM ->
                0;
           true ->
                State#state.seq_num
        end,
    case gen_tcp:send(
           State#state.socket,
           ?PACKET_REQUEST(SeqNum, DeadLine, Request)) of
        ok ->
            ok = register_request_from_local_process(
                   State#state.registry, RequestRef,
                   From, SeqNum),
            {noreply, State#state{seq_num = SeqNum + 1}};
        {error, Reason} ->
            %% Failed to send. Reply to the local process
            %% immediately and try to reconnect.
            _Sent = From ! ?ARRIVE_ERROR(RequestRef, Reason),
            {noreply, connect(State)}
    end;
handle_cast(?QUEUE_REQUEST(From, RequestRef, _DeadLine, _Request), State) ->
    %% not connected. Reply to the caller immediately.
    _Sent = From ! ?ARRIVE_ERROR(RequestRef, not_connected),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
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
    %% the function is called only in 'client' mode, so
    %% clear the registry
    ok = clear_client_registry(State#state.registry),
    {host, Host} = lists:keyfind(host, 1, State#state.options),
    {port, Port} = lists:keyfind(port, 1, State#state.options),
    ConnTimeout =
        proplists:get_value(conn_timeout, State#state.options, 2000),
    SocketOptions =
        [binary, {packet, 4}, {active, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, SocketOptions, ConnTimeout) of
        {ok, Socket} ->
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
        Registry :: registry(),
        RequestRef :: reference(),
        From :: pid(),
        SeqNum :: seq_num()) -> ok.
register_request_from_local_process(Registry, RequestRef, From, SeqNum) ->
    true = ets:insert(Registry, {SeqNum, From, RequestRef}),
    ok.

%% @doc Lookup RequestRef by the SeqNum and remove the record.
-spec pop_request_ref(Registry :: registry(), SeqNum :: seq_num()) ->
                             {ok,
                              RequestRef :: reference(),
                              From :: pid()} |
                             undefined.
pop_request_ref(Registry, SeqNum) ->
    case ets:lookup(Registry, SeqNum) of
        [{SeqNum, From, RequestRef}] ->
            true = ets:delete(Registry, SeqNum),
            {ok, RequestRef, From};
        [] ->
            undefined
    end.

%% @doc Clear the clients registry.
%% All records will be deleted and all pending requests will
%% be discarded, sending the answers like {error, disconnected}.
-spec clear_client_registry(Registry :: registry()) -> ok.
clear_client_registry(Registry) ->
    Reason = disconnected,
    undefined =
        ets:foldl(
          fun({_SeqNum, From, RequestRef}, Accum) ->
                  _Sent = From ! ?ARRIVE_ERROR(RequestRef, Reason),
                  Accum
          end, undefined, Registry),
    true = ets:delete_all_objects(Registry),
    ok.

%% @doc Handle a data packet received from the network socket.
-spec handle_data_from_net(State :: #state{}, Data :: binary()) -> ok.
handle_data_from_net(State, ?PACKET_REPLY(SeqNum, Reply)) ->
    case pop_request_ref(State#state.registry, SeqNum) of
        {ok, RequestRef, From} ->
            _Sent = From ! ?ARRIVE_REPLY(RequestRef, Reply),
            ok;
        undefined ->
            %% ignore
            ok
    end;
handle_data_from_net(State, ?PACKET_ERROR(SeqNum, EncodedReason)) ->
    case pop_request_ref(State#state.registry, SeqNum) of
        {ok, RequestRef, From} ->
            _Sent = From ! ?ARRIVE_ERROR(RequestRef, EncodedReason),
            ok;
        undefined ->
            %% ignore
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
            Len;
        undefined ->
            undefined
    end.
