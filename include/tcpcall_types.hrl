%%%-------------------------------------------------------------------
%%% File        : tcpcall_types.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : tcpcall internal types definitions
%%% Created     : 12 Nov 2014
%%%-------------------------------------------------------------------

-ifndef(_TCPCALL_TYPES).
-define(_TCPCALL_TYPES, true).

-define(MAX_SEQ_NUM, 16#ffffffff).

-type seq_num() :: 0..?MAX_SEQ_NUM.

-type registry() :: ets:tab().

-endif.
