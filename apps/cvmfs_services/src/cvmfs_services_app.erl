%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_services_app public API
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_services_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    case application:get_env(cvmfs_services, repo_config) of
        {ok, {file, RepoConfigFile}} ->
            {ok, VarList} = file:consult(RepoConfigFile),
            Vars = maps:from_list(VarList);
        {ok, RepoConfigMap} ->
            Vars = RepoConfigMap
    end,

    cvmfs_services_sup:start_link({maps:get(repos, Vars)
                                  ,maps:get(acl, Vars)}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
