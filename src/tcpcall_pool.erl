%%% @doc
%%% Client connection pool.
%%% Provides a load balancer feature to the tcpcall library.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 13 Sep 2015
%%% @copyright 2015, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall_pool).

-behaviour(gen_server).

%% API exports
-export([start_link/2, call/3, cast/2, stop/1, workers/1, reconfig/2]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("tcpcall.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

%% Process state record
-record(
   state,
   {name :: tcpcall:client_pool_name(),
    options = [] :: tcpcall:client_pool_options(),
    getter_state :: getter_state(),
    peers = [] :: peers_registry(),
    workers = dict:new() :: workers_registry(),
    balancer = ?round_robin :: tcpcall:balancer()
   }).

-type getter_state() ::
        {tcpcall:peers_getter(),
         Seconds :: pos_integer(),
         TimerRef :: timer:tref()} |
        undefined.

-type peers_registry() :: [enum_peer()].

-type enum_peer() :: {pos_integer(), tcpcall:peer()}.

-type workers_registry() ::
        dict:dict(pid(), {IsConnected :: boolean(), enum_peer()}).

%% ETS table keys
-define(WORKERS, workers).
-define(POINTER, pointer).

%% Internal signals
-define(SIG_SPAWN(Peer), {spawn, Peer}).
-define(SIG_RECONFIG(Options), {reconfig, Options}).
-define(SIG_GET_PEERS, get_peers).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start connection pool process.
%% The process can be started as part of the supervision tree.
-spec start_link(PoolName :: tcpcall:client_pool_name(),
                 Options :: tcpcall:client_pool_options()) ->
                        {ok, Pid :: pid()} | ignore |
                        {error, Reason :: any()}.
start_link(PoolName, Options) ->
    gen_server:start_link(
      {local, PoolName}, ?MODULE,
      _Args = {PoolName, Options}, _GenServerOptions = []).

%% @doc Make a Request-Reply interaction with a remote
%% server through the started connection pool.
%% See tcpcall:connect_pool/2 function description for more details.
-spec call(PoolName :: tcpcall:client_pool_name(),
           Request :: tcpcall:data(),
           Timeout :: pos_integer()) ->
                  {ok, Reply :: any()} |
                  {error, Reason :: any()}.
call(PoolName, Request, Timeout) ->
    case ets:lookup(PoolName, ?WORKERS) of
        [{?WORKERS, _Balancer, [Worker]}] ->
            %% there is only one worker, so make the call
            %% without any balancing
            tcpcall:call(Worker, Request, Timeout);
        [{?WORKERS, ?round_robin = Balancer, [_ | _] = Workers}] ->
            UpdateOp =
                {_Position = 2, _Increment = 1,
                 _Threshold = length(Workers), _SetValue = 1},
            Index = ets:update_counter(PoolName, ?POINTER, UpdateOp),
            Workers2 =
                if Index > 1 ->
                        %% Reorder worker list such way if the element,
                        %% indexed by the pointer is the very first element
                        %% of the worker list.
                        {L1, L2} = lists:split(Index - 1, Workers),
                        L2 ++ L1;
                   true ->
                        Workers
                end,
            call_loop(Balancer, Workers2, Request, Timeout);
        [{?WORKERS, ?random = Balancer, [_ | _] = Workers}] ->
            call_loop(Balancer, Workers, Request, Timeout);
        _ ->
            {error, not_connected}
    end.

%% @doc Make an asynchronous (without a response) interaction with a remote
%% server through the started connection pool.
%% See tcpcall:connect_pool/2 function description for more details.
-spec cast(PoolName :: tcpcall:client_pool_name(), Request :: tcpcall:data()) ->
                  ok | {error, Reason :: any()}.
cast(PoolName, Request) ->
    case ets:lookup(PoolName, ?WORKERS) of
        [{?WORKERS, _Balancer, [Worker]}] ->
            %% there is only one worker, so make the cast
            %% without any balancing and failover
            tcpcall:cast(Worker, Request);
        [{?WORKERS, ?round_robin = Balancer, [_ | _] = Workers}] ->
            UpdateOp =
                {_Position = 2, _Increment = 1,
                 _Threshold = length(Workers), _SetValue = 1},
            Index = ets:update_counter(PoolName, ?POINTER, UpdateOp),
            Workers2 =
                if Index > 1 ->
                        %% Reorder worker list such way if the element,
                        %% indexed by the pointer is the very first element
                        %% of the worker list.
                        {L1, L2} = lists:split(Index - 1, Workers),
                        L2 ++ L1;
                   true ->
                        Workers
                end,
            cast_loop(Balancer, Workers2, Request);
        [{?WORKERS, ?random = Balancer, [_ | _] = Workers}] ->
            cast_loop(Balancer, Workers, Request);
        _ ->
            {error, not_connected}
    end.

%% @doc Tell the pool process to stop.
-spec stop(PoolName :: tcpcall:client_pool_name()) -> ok.
stop(PoolName) ->
    _Sent = PoolName ! ?SIG_STOP,
    ok.

%% @doc Return process ID list of all active workers.
-spec workers(PoolName :: tcpcall:client_pool_name()) -> [pid()].
workers(PoolName) ->
    [{?WORKERS, _Balancer, Workers}] = ets:lookup(PoolName, ?WORKERS),
    Workers.

%% @doc Apply new client pool configuration.
-spec reconfig(PoolName :: tcpcall:client_pool_name(),
               NewOptions :: tcpcall:client_pool_options()) -> ok.
reconfig(PoolName, NewOptions) ->
    gen_server:cast(PoolName, ?SIG_RECONFIG(NewOptions)).

%% --------------------------------------------------------------------
%% gen_server callback functions
%% --------------------------------------------------------------------

%% @hidden
-spec init({tcpcall:client_pool_name(),
            tcpcall:client_pool_options()}) ->
                  {ok, InitialState :: #state{}}.
init({PoolName, Options}) ->
    false = process_flag(trap_exit, true),
    PoolName = ets:new(PoolName, [named_table, public]),
    %% initialize round robin pointer
    true = ets:insert(PoolName, {?POINTER, 0}),
    {ok, configure(#state{name = PoolName}, Options)}.

%% @hidden
-spec handle_info(Request :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), NewState :: #state{}}.
handle_info(?SIG_SPAWN({_No, {Host, Port}} = Peer), State) ->
    case lists:member(Peer, State#state.peers) of
        true ->
            case tcpcall:connect(
                   [{host, Host}, {port, Port}, {lord, self()}]) of
                {ok, Pid} ->
                    NewState =
                        State#state{
                          workers = dict:store(
                                      Pid, {_IsConnected = false, Peer},
                                      State#state.workers)
                         },
                    ok = publish_workers(NewState),
                    {noreply, NewState};
                _Error ->
                    ok = schedule_spawn(Peer, 1000),
                    {noreply, State}
            end;
        false ->
            %% Unknown worker ID, ignore it
            {noreply, State}
    end;
handle_info({'EXIT', From, _Reason}, State) ->
    case dict:find(From, State#state.workers) of
        {ok, {_IsConnected, Peer}} ->
            %% One of our workers is died
            ok = schedule_spawn(Peer, 100),
            NewState =
                State#state{
                  workers = dict:erase(From, State#state.workers)
                 },
            ok = publish_workers(NewState),
            {noreply, NewState};
        error ->
            {noreply, State}
    end;
handle_info({tcpcall_client, Pid, IsConnected}, State) ->
    %% Received connection status notification from worker
    case dict:find(Pid, State#state.workers) of
        {ok, {IsConnected, _}} ->
            %% we already know about it, just ignore
            {noreply, State};
        {ok, {_, Peer}} ->
            %% connection status changed
            NewWorkers =
                dict:store(Pid, {IsConnected, Peer}, State#state.workers),
            NewState = State#state{workers = NewWorkers},
            ok = publish_workers(NewState),
            {noreply, NewState};
        error ->
            %% notification from unregistered worker, ignore it
            {noreply, State}
    end;
handle_info(?SIG_GET_PEERS, State) ->
    case State#state.getter_state of
        {_GetterFun, _Period, _Reference} ->
            {noreply, apply_peers(State)};
        undefined ->
            {noreply, State}
    end;
handle_info(?SIG_STOP, State) ->
    {stop, _Reason = normal, State};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(?SIG_RECONFIG(Options), State) ->
    {noreply, configure(State, Options)};
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, State) ->
    _Ignored =
        [catch tcpcall_client:stop(Worker) ||
            Worker <- dict:fetch_keys(State#state.workers)],
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Reconfigure the pool according to new options.
-spec configure(State :: #state{},
                Options :: tcpcall:client_pool_options()) ->
                       NewState :: #state{}.
configure(State, Options) ->
    apply_peers(apply_getter(State#state{options = Options})).

%% @doc Apply new peer list getter function.
-spec apply_getter(State :: #state{}) ->
                          NewState :: #state{}.
apply_getter(State) ->
    NewPeersGetter =
        case [{PG, P} || {peers, PG, P} <- State#state.options] of
            [Elem | _] -> Elem;
            [] -> undefined
        end,
    NewPeersGetterState =
        case {State#state.getter_state, NewPeersGetter} of
            {{F, P, _Ref} = PeersGetterState, {F, P}} ->
                %% configuration not changed
                PeersGetterState;
            {{_F, _P, Ref}, {F, P}} ->
                %% configuration changed. Drop old timer, set up
                %% new one
                {ok, cancel} = timer:cancel(Ref),
                {ok, NewRef} = timer:send_interval(P * 1000, ?SIG_GET_PEERS),
                {F, P, NewRef};
            {{_F, _P, Ref}, undefined} ->
                {ok, cancel} = timer:cancel(Ref),
                undefined;
            {_, {F, P}} ->
                {ok, NewRef} = timer:send_interval(P * 1000, ?SIG_GET_PEERS),
                {F, P, NewRef};
            {_, _} ->
                undefined
        end,
    State#state{getter_state = NewPeersGetterState}.

%% @doc Apply new peer list.
-spec apply_peers(State :: #state{}) -> NewState :: #state{}.
apply_peers(State) ->
    Options = State#state.options,
    OldPeerList = State#state.peers,
    NewPeerList =
        enumerate(
          case State#state.getter_state of
              {GetterFun, _Period, _Reference} ->
                  GetterFun();
              undefined ->
                  proplists:get_value(peers, Options, [])
          end),
    State1 = remove_peers(State, OldPeerList -- NewPeerList),
    State2 = add_peers(State1, NewPeerList -- OldPeerList),
    State3 =
        State2#state{
          balancer = proplists:get_value(balancer, Options, ?round_robin)},
    ok = publish_workers(State3),
    State3.

%% @doc Helper for the apply_peers/1 fun.
%% Add new peers to configuration.
-spec add_peers(State :: #state{}, Peers :: [enum_peer()]) ->
                       NewState :: #state{}.
add_peers(State, Peers) ->
    lists:foldl(
      fun(Peer, Accum) ->
              _Sent = self() ! ?SIG_SPAWN(Peer),
              Accum#state{peers = [Peer | Accum#state.peers]}
      end, State, Peers).

%% @doc Helper for the apply_peers/1 fun.
%% Remove existing peers from configuration.
-spec remove_peers(State :: #state{}, Peers :: [enum_peer()]) ->
                          NewState :: #state{}.
remove_peers(State, Peers) ->
    lists:foldl(
      fun(Peer, Accum) ->
              NewWorkers =
                  dict:filter(
                    fun(Pid, {_IsConnected, P}) when P == Peer ->
                            ok = tcpcall_client:stop(Pid),
                            false;
                       (_, _) ->
                            true
                    end, State#state.workers),
              Accum#state{
                peers = Accum#state.peers -- [Peer],
                workers = NewWorkers}
      end, State, Peers).

%% @doc Convert list with non unique elements to list of {No, Term}
%% where No is number of element within group of equal elements.
%% For example, [a, a, a, b, c, c] will be converted to
%% [{1, a}, {2, a}, {3, a}, {1, b}, {2, b}, {1, c}]
-spec enumerate([any()]) -> [{pos_integer(), any()}].
enumerate(Terms) ->
    lists:append(
      [begin
           L = [T || T <- Terms, T == Term],
           lists:zip(lists:seq(1, length(L)), L)
       end || Term <- lists:usort(Terms)]).

%% @doc Try workers in order according to configured load balancer
%% type, make failover to the next worker if current one is not
%% connected or overloaded.
%% Helper for the call/3 API function.
-spec call_loop(BalancerType :: tcpcall:balancer(),
                Workers :: [pid()],
                Request :: tcpcall:data(),
                Timeout :: pos_integer()) ->
                       {ok, Reply :: any()} |
                       {error, Reason :: any()}.
call_loop(_Balancer, [], _Request, _Timeout) ->
    {error, not_connected};
call_loop(_Balancer, [Worker], Request, Timeout) ->
    tcpcall:call(Worker, Request, Timeout);
call_loop(?random = Balancer, Workers, Request, Timeout) ->
    %% There is two ways to implement failover using
    %% random workers: 1. shuffle the whole worker list before
    %% traversing; 2. pop random element on each try.
    %% I've selected latter because failover is not a usual
    %% case, so it is better to play with pop/join as less
    %% as possible.
    {Worker, Tail} = pop_random(Workers),
    case tcpcall:call(Worker, Request, Timeout) of
        {ok, _Reply} = Ok ->
            Ok;
        {error, overload} when Tail /= [] ->
            call_loop(Balancer, Tail, Request, Timeout);
        {error, overload} = OverloadError ->
            %% raise overload error up to the caller
            OverloadError;
        {error, not_connected} ->
            call_loop(Balancer, Tail, Request, Timeout);
        {error, _Reason} = Error ->
            Error
    end;
call_loop(?round_robin = Balancer, [Worker | Tail], Request, Timeout) ->
    case tcpcall:call(Worker, Request, Timeout) of
        {ok, _Reply} = Ok ->
            Ok;
        {error, overload} when Tail /= [] ->
            call_loop(Balancer, Tail, Request, Timeout);
        {error, overload} = OverloadError ->
            %% raise overload error up to the caller
            OverloadError;
        {error, not_connected} ->
            call_loop(Balancer, Tail, Request, Timeout);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Try workers in order according to configured load balancer
%% type, make failover to the next worker if current one is not
%% connected or overloaded.
%% Helper for the cast/2 API function.
-spec cast_loop(BalancerType :: tcpcall:balancer(),
                Workers :: [pid()],
                Request :: tcpcall:data()) ->
                       ok | {error, Reason :: any()}.
cast_loop(_Balancer, [], _Request) ->
    {error, not_connected};
cast_loop(_Balancer, [Worker], Request) ->
    tcpcall:cast(Worker, Request);
cast_loop(?random = Balancer, Workers, Request) ->
    %% There is two ways to implement failover using
    %% random workers: 1. shuffle the whole worker list before
    %% traversing; 2. pop random element on each try.
    %% I've selected latter because failover is not a usual
    %% case, so it is better to play with pop/join as less
    %% as possible.
    {Worker, Tail} = pop_random(Workers),
    case tcpcall:cast(Worker, Request) of
        ok ->
            ok;
        {error, overload} when Tail /= [] ->
            cast_loop(Balancer, Tail, Request);
        {error, overload} = OverloadError ->
            %% raise overload error up to the caller
            OverloadError;
        {error, not_connected} ->
            cast_loop(Balancer, Tail, Request);
        {error, _Reason} = Error ->
            Error
    end;
cast_loop(?round_robin = Balancer, [Worker | Tail], Request) ->
    case tcpcall:cast(Worker, Request) of
        ok ->
            ok;
        {error, overload} when Tail /= [] ->
            cast_loop(Balancer, Tail, Request);
        {error, overload} = OverloadError ->
            %% raise overload error up to the caller
            OverloadError;
        {error, not_connected} ->
            cast_loop(Balancer, Tail, Request);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Fetch random element from the list. Return tuple of two elements:
%% fetched element and origin list without the element.
-spec pop_random([any(), ...]) -> {Elem :: any(), Rest :: list()}.
pop_random([Elem]) ->
    {Elem, []};
pop_random([_ | _] = List) ->
    %% seed RNG only when not seeded yet
    _Ignored =
        case get(random_seed) == undefined of
            true ->
                random:seed(os:timestamp());
            false -> ok
        end,
    Index = random:uniform(length(List)),
    {List1, [Elem | List2]} = lists:split(Index - 1, List),
    {Elem, List1 ++ List2}.

%% @doc Schedule worker respawn.
-spec schedule_spawn(enum_peer(), AfterMillis :: non_neg_integer()) -> ok.
schedule_spawn(Peer, AfterMillis) ->
    {ok, _TRef} = timer:send_after(AfterMillis, ?SIG_SPAWN(Peer)),
    ok.

%% @doc Publish connected workers with shared ETS table.
-spec publish_workers(#state{}) -> ok.
publish_workers(State) ->
    ConnectedWorkers =
        dict:fold(
          fun(Pid, {_Connected = true, _Peer}, Acc) ->
                  [Pid | Acc];
             (_, _, Acc) ->
                  Acc
          end, [], State#state.workers),
    true = ets:insert(
             State#state.name,
             {?WORKERS, State#state.balancer, ConnectedWorkers}),
    ok.

%% ----------------------------------------------------------------------
%% EUnit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

pop_random_test() ->
    Origin = "qwertyuiopasdfghjklzxcvbnm",
    %% shuffle(list()) implementation, based on pop_random/1
    {Shuffled, []} =
        lists:foldl(
          fun(_, {Accum, List}) ->
                  {Elem, Rest} = pop_random(List),
                  {[Elem | Accum], Rest}
          end, {[], Origin}, Origin),
    ?assertNotMatch(Origin, Shuffled),
    ?assertNotMatch(Origin, lists:reverse(Shuffled)),
    ?assertEqual(lists:sort(Origin), lists:sort(Shuffled)).

enumerate_test_() ->
    [?_assertMatch([], enumerate([])),
     ?_assertMatch([{1, a}], enumerate([a])),
     ?_assertMatch(
        [{1, a}, {2, a}, {3, a}, {1, b}, {1, c}, {2, c}],
        enumerate([a, a, a, b, c, c]))].

-endif.
