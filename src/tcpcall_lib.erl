%%% @doc
%%% Common functions.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 12 Nov 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(tcpcall_lib).

%% API exports
-export(
   [micros/0,
    millis/0
   ]).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Micros elapsed since Unix Epoch.
-spec micros() -> pos_integer().
micros() ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    (MegaSeconds * 1000000 + Seconds) * 1000000 + MicroSeconds.

%% @doc Millis elapsed since Unix Epoch.
-spec millis() -> pos_integer().
millis() ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    (MegaSeconds * 1000000 + Seconds) * 1000 + MicroSeconds div 1000.
