%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_repo_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_repo_handler).

-export([init/2]).

init(Req0, State) ->
    Repos = cvmfs_auth:get_repos(),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"text/plain">>},
                           jsx:encode(#{<<"repos">> => Repos}),
                           Req0),
    {ok, Req, State}.

