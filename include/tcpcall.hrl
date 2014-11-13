%%%-------------------------------------------------------------------
%%% File        : tcpcall.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : tcpcall definitions file
%%% Created     : 10 Nov 2014
%%%-------------------------------------------------------------------

-ifndef(_TCPCALL).
-define(_TCPCALL, true).

-define(SIG_STOP, stop).

%% ----------------------------------------------------------------------
%% debugging

-ifdef(TRACE).
-define(
   trace(Format, Args),
   ok = io:format(
          "TRACE> mod:~w; line:~w; pid:~w; msg:" ++ Format ++ "~n",
          [?MODULE, ?LINE, self() | Args]
         )).
-else.
-define(trace(F, A), ok).
-endif.

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
