%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_root_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_root_handler).

-export([init/2]).

init(Req0, State) ->
    Banner = <<"You are in an open field on the west side of a white house with a boarded front door.">>,
    API = #{<<"banner">> => Banner,
            <<"resources">> => [<<"users">>, <<"repos">>, <<"leases">>]},
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           jsx:encode(API),
                           Req0),
    {ok, Req, State}.

