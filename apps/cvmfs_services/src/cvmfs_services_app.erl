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
    RepoVars = read_vars(repo_config, #{repos => [], keys => []}),

    UserVars = read_vars(user_config,
                         #{max_lease_time => 7200000,
                           receiver_config => [{size, 1}, {max_overflow, 1}],
                           receiver_worker_config => [{executable_path, "/usr/bin/cvmfs_receiver"}]}),

    application:set_env(cvmfs_services, max_lease_time, maps:get(max_lease_time, UserVars)),

    ReceiverPoolConfig1 = maps:get(receiver_config, UserVars),
    ReceiverPoolConfig2 = case lists:keyfind(worker_module, 1, ReceiverPoolConfig1) of
                              false ->
                                  [{worker_module, cvmfs_receiver} | ReceiverPoolConfig1];
                              _ ->
                                  ReceiverPoolConfig1
                          end,
    ReceiverWorkerConfig = maps:from_list(maps:get(receiver_worker_config, UserVars)),

    {ok, Services} = application:get_env(enabled_services),

    case lists:member(cvmfs_fe, Services) of
        true ->
            {ok, _} = cvmfs_fe:start_link();
        false ->
            ok
    end,

    Services2 = lists:delete(cvmfs_fe, Services),

    cvmfs_services_sup:start_link({Services2,
                                   maps:get(repos, RepoVars),
                                   maps:get(keys, RepoVars),
                                   ReceiverPoolConfig2,
                                   ReceiverWorkerConfig}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
read_vars(VarName, Defaults) ->
    case application:get_env(VarName) of
        {ok, {file, ConfigFile}} ->
            {ok, VarList} = file:consult(ConfigFile),
            maps:from_list(VarList);
        {ok, ConfigMap} ->
            ConfigMap;
        undefined ->
            Defaults
    end.
