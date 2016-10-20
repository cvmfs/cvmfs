%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_user_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_user_handler).

-export([init/2]).

init(Req0, State) ->
    Users = cvmfs_auth:get_users(),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           jsx:encode(#{<<"users">> => Users}),
                           Req0),
    {ok, Req, State}.

