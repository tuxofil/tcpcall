%%%-------------------------------------------------------------------
%%% File        : tcpcall_proto.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : tcpcall protocol definitions file
%%% Created     : 12 Nov 2014
%%%-------------------------------------------------------------------

-ifndef(_TCPCALL_PROTO).
-define(_TCPCALL_PROTO, true).

%% message type markers
-define(REQUEST, 0).
-define(REPLY, 1).
-define(ERROR, 2).

%% packet formats
-define(
   PACKET_REQUEST(SeqNum, DeadLine, Request),
   <<?REQUEST:8/big-unsigned,
     SeqNum:32/big-unsigned,
     DeadLine:64/big-unsigned,
     Request/binary>>).

-define(
   PACKET_REPLY(SeqNum, Reply),
   <<?REPLY:8/big-unsigned,
     SeqNum:32/big-unsigned,
     Reply/binary>>).

-define(
   PACKET_ERROR(SeqNum, Reason),
   <<?ERROR:8/big-unsigned,
     SeqNum:32/big-unsigned,
     Reason/binary>>).

-endif.
