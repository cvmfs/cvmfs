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
    UserVars = cvmfs_config:read(user_config, cvmfs_config:default_user_config()),

    LogLevel = maps:get(log_level, UserVars, <<"info">>),
    ok = set_lager_log_level(LogLevel),

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
                                  ReceiverPoolConfig2,
                                  ReceiverWorkerConfig}).


stop(_State) ->
    ok.


%%====================================================================
%% Private functions
%%====================================================================

set_lager_log_level(LogLevel) ->
    Levels = [<<"debug">>, <<"info">>, <<"notice">>, <<"warning">>, <<"error">>,
              <<"critical">>, <<"alert">>, <<"emergency">>],
    case lists:member(LogLevel, Levels) of
        true ->
            lager:set_loglevel({lager_syslog_backend, {"cvmfs_gateway", local1}},
                               erlang:binary_to_atom(LogLevel, latin1)),
            ok;
        false ->
            error
    end.