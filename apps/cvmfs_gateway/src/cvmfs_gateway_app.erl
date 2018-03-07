%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_gateway_app public API
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_gateway_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).


%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    RepoVars = cvmfs_app_util:read_vars(repo_config,
                                        cvmfs_app_util:default_repo_config()),

    UserVars = cvmfs_app_util:read_vars(user_config,
                                        cvmfs_app_util:default_user_config()),

    application:set_env(cvmfs_gateway, max_lease_time,
                        maps:get(max_lease_time, UserVars) * 1000),

    ReceiverPoolConfig1 = maps:get(receiver_config, UserVars),
    ReceiverPoolConfig2 = maps:to_list(
                            case maps:is_key(worker_module, ReceiverPoolConfig1) of
                                false ->
                                    maps:put(worker_module, cvmfs_receiver, ReceiverPoolConfig1);
                                true ->
                                    ReceiverPoolConfig1
                            end),
    ReceiverWorkerConfig = maps:get(receiver_worker_config, UserVars),

    {ok, Services} = application:get_env(enabled_services),

    TcpPort = maps:get(fe_tcp_port, UserVars),

    case lists:member(cvmfs_fe, Services) of
        true ->
            {ok, _} = cvmfs_fe:start_link([TcpPort]);
        false ->
            ok
    end,

    Services2 = lists:delete(cvmfs_fe, Services),

    cvmfs_gateway_sup:start_link({Services2,
                                   maps:get(repos, RepoVars),
                                   maps:get(keys, RepoVars),
                                   ReceiverPoolConfig2,
                                   ReceiverWorkerConfig}).


stop(_State) ->
    ok.

