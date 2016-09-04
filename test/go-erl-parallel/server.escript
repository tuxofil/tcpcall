#!/usr/bin/env escript
%%! -pa ../../erlang/ebin
main(_) ->
    {ok, _} = application:ensure_all_started(tcpcall),
    {ok, _} = tcpcall:listen(
                [{bind_port, 5000},
                 {receiver,
                  fun(B) ->
                          I = binary_to_integer(B),
                          ok = timer:sleep(I * 500),
                          integer_to_binary(I + 1)
                  end}]),
    ok = timer:sleep(1000 * 120).
