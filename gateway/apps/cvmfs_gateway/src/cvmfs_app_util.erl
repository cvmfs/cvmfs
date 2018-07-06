%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_app_utl public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_app_util).

-export([get_max_lease_time/0,
         read_config_file/1,
         read_vars/2,
         default_repo_config/0,
         default_user_config/0]).


get_max_lease_time() ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_gateway, max_lease_time),
    MaxLeaseTime.


read_config_file(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            jsx:decode(Data, [{labels, atom}, return_maps]);
        {error, Reason} ->
            {error, Reason}
    end.


read_vars(VarName, Defaults) ->
    case application:get_env(VarName) of
        {ok, {file, ConfigFile}} ->
            read_config_file(ConfigFile);
        {ok, ConfigMap} ->
            ConfigMap;
        undefined ->
            Defaults
    end.


default_repo_config() ->
    #{repos => [], keys => []}.


default_user_config() ->
    #{max_lease_time => 7200,
      fe_tcp_port => 4929,
      receiver_config => #{size => 1,
                           max_overflow => 1},
      receiver_worker_config =>
          #{executable_path => "/usr/bin/cvmfs_receiver"},
      log_level => <<"info">>}.

