-module(cvmfs_fe_test_handler).

-export([init/2]).

init(Req0, State) ->
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           <<"Hello from CVMFS!">>,
                           Req0),
    {ok, Req, State}.
