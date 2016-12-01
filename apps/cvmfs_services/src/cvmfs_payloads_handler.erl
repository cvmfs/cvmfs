%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_payloads_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_payloads_handler).

-export([init/2]).

init(Req0 = #{method := <<"GET">>}, State) ->
    Req1 = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/plain-text">>},
                           <<"">>,
                           Req0),
    {ok, Req1, State};
init(Req0 = #{method := <<"POST">>}, State) ->
    {ok, Req0, State}.


