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
    Repos = cvmfs_auth:get_repos(),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"application/json">>},
                           jsx:encode(#{<<"repos">> => Repos}),
                           Req0),
    {ok, Req, State}.

