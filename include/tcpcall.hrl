%%%-------------------------------------------------------------------
%%% File        : tcpcall.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : tcpcall definitions file
%%% Created     : 10 Nov 2014
%%%-------------------------------------------------------------------

-ifndef(_TCPCALL).
-define(_TCPCALL, true).

-define(SIG_STOP, stop).

%% Connection pool balancer types
-define(round_robin, round_robin).
-define(random, random).

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
