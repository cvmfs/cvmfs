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
                         #{max_lease_time => 7200,
                           fe_tcp_port => 8080,
                           receiver_config => #{size => 1,
                                                max_overflow => 1},
                           receiver_worker_config =>
                               #{executable_path => "/usr/bin/cvmfs_receiver"}}),

    application:set_env(cvmfs_services, max_lease_time,
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
            case file:read_file(ConfigFile) of
                {ok, Data} ->
                    jsx:decode(Data, [{labels, atom}, return_maps]);
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, ConfigMap} ->
            ConfigMap;
        undefined ->
            Defaults
    end.
