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
    Vars = case application:get_env(repo_config) of
               {ok, {file, RepoConfigFile}} ->
                   {ok, VarList} = file:consult(RepoConfigFile),
                   maps:from_list(VarList);
               {ok, RepoConfigMap} ->
                   RepoConfigMap;
               undefined ->
                   #{repos => [], keys => []}
           end,
    ReceiverPoolConfig = case application:get_env(receiver_config) of
                             {ok, PoolConfig} ->
                                 PoolConfig;
                             undefined ->
                                 []
                         end,
    ReceiverWorkerConfig = case application:get_env(receiver_worker_config) of
                               {ok, WorkerConfig} ->
                                   maps:from_list(WorkerConfig);
                               undefined ->
                                   #{}
                           end,
    {ok, Services} = application:get_env(enabled_services),

    cvmfs_services_sup:start_link({Services,
                                   maps:get(repos, Vars),
                                   maps:get(keys, Vars),
                                   ReceiverPoolConfig,
                                   ReceiverWorkerConfig}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
