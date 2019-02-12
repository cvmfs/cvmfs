%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to configuration
%%% @end
%%%-------------------------------------------------------------------

-module(config).

-export([read_var/2,
         default_repo_config/0,
         default_user_config/0,
         get_repo_names/1]).

read_var(VarName, Defaults) ->
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


get_repo_names(RepoVars) ->
    Extract = fun(#{domain := RepoName}) ->
                    RepoName;
                 (RepoName) when is_binary(RepoName) ->
                    RepoName
              end,
    lists:map(Extract, RepoVars).


%%% Private functions

read_config_file(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            jsx:decode(Data, [{labels, atom}, return_maps]);
        {error, Reason} ->
            {error, Reason}
    end.


