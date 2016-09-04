#!/usr/bin/env escript
%%! -pa ../../erlang/ebin
main(_) ->
    {ok, _} = application:ensure_all_started(tcpcall),
    {ok, _} = tcpcall:listen(
                [{bind_port, 5000},
                 {receiver,
                  fun(B) -> integer_to_binary(binary_to_integer(B) * 2) end}]),
    ok = timer:sleep(1000 * 120).
