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

-record(state,
        {socket :: port(),
         receiver :: tcpcall:receiver()}).

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
    case lists:keyfind(name, 1, Options) of
        {name, RegisteredName} ->
            true = register(RegisteredName, self());
        false ->
            ok
    end,
    {bind_port, BindPort} = lists:keyfind(bind_port, 1, Options),
    {receiver, Receiver} = lists:keyfind(receiver, 1, Options),
    SocketOpts =
        [{active, false}, binary, {reuseaddr, true}, {packet, 4},
         {keepalive, true}],
    {ok, Socket} = gen_tcp:listen(BindPort, SocketOpts),
    ok = schedule_accept(),
    {ok, _State = #state{socket = Socket,
                         receiver = Receiver}}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), State :: #state{}}.
handle_info(?SIG_ACCEPT, State) ->
    case gen_tcp:accept(State#state.socket, 100) of
        {ok, Socket} ->
            ok = tcpcall_server:start(
                   [{socket, Socket},
                    {acceptor, self()},
                    {receiver, State#state.receiver}]);
        {error, timeout} ->
            ok
    end,
    ok = schedule_accept(),
    {noreply, State};
handle_info(?SIG_STOP, State) ->
    {stop, _Reason = normal, State};
handle_info(_Request, State) ->
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

%% @doc Schedule new TCP connection accept.
-spec schedule_accept() -> ok.
schedule_accept() ->
    _Sent = self() ! ?SIG_ACCEPT,
    ok.
