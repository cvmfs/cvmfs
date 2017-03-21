%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_repos_handler - request handler for the "repos" resource
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_repos_handler).

-export([init/2]).

%%--------------------------------------------------------------------
%% @doc
%% Handles requests for the /api/resource resource, returning a list
%% of registered repositories
%%
%% @end
%%--------------------------------------------------------------------
init(Req0, State) ->
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    Repos = cvmfs_be:get_repos(),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"application/json">>},
                           jsx:encode(#{<<"repos">> => Repos}),
                           Req0),

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, Req, State}.

