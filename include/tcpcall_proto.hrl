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
-define(CAST, 3).
-define(FLOW_CONTROL_SUSPEND, 4).
-define(FLOW_CONTROL_RESUME, 5).
-define(UPLINK_CAST, 6).

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

-define(
   PACKET_CAST(SeqNum, Request),
   <<?CAST:8/big-unsigned,
     SeqNum:32/big-unsigned,
     Request/binary>>).

-define(
   PACKET_FLOW_CONTROL_SUSPEND(Millis),
   <<?FLOW_CONTROL_SUSPEND:8/big-unsigned,
     Millis:64/big-unsigned>>).

-define(
   PACKET_FLOW_CONTROL_RESUME,
   <<?FLOW_CONTROL_RESUME:8/big-unsigned>>).

-define(
   PACKET_UPLINK_CAST(Data),
   <<?UPLINK_CAST:8/big-unsigned,
     Data/binary>>).

-endif.
