%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_root_handler - request handler for the API root
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_root_handler).

-export([init/2]).

%%--------------------------------------------------------------------
%% @doc
%% Handles requests for the /api resource
%%
%% Return a list of available resources.
%%
%% @end
%%--------------------------------------------------------------------
init(Req0, State) ->
    Uid = cvmfs_be:unique_id(),
    {URI, T0} = cvmfs_fe_util:tick(Uid, <<"GET">>, Req0, micro_seconds),

    Banner = <<"You are in an open field on the west side of a white house with a boarded front door.">>,
    API = #{<<"banner">> => Banner,
            <<"resources">> => [<<"users">>, <<"repos">>, <<"leases">>, <<"payloads">>]},
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           jsx:encode(API),
                           Req0),

    cvmfs_fe_util:tock(Uid, <<"GET">>, URI, T0, micro_seconds),
    {ok, Req, State}.

