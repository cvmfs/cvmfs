%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
<<<<<<< 8f9f0f7588f2c92dbf2d00b6b1434dc08d6a53f6
%%% @doc cvmfs_fe_test_handler
=======
%%% @doc cvmfs_fe_root_handler
>>>>>>> Fre Nov  4 17:29:01 CET 2016
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_root_handler).

-export([init/2]).

init(Req0, State) ->
    Banner = <<"You are in an open field on the west side of a white house with a boarded front door.">>,
    API = #{<<"banner">> => Banner,
            <<"resources">> => [<<"user">>, <<"repo">>, <<"session">>, <<"lease">>, <<"job">>]},
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           jsx:encode(API),
                           Req0),
    {ok, Req, State}.

