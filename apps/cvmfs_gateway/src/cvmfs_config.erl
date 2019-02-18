%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to configuration
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_config).

-include_lib("eunit/include/eunit.hrl").

-export([read/2,
         default_repo_config/0,
         default_user_config/0,
         load/1,
         load/2]).


% Read application configuration (either user_config or repo_config)
%
% Read an application configuration variable or return a default value
% if the variable is undefined. If the variable is set to {file, FileName},
% FileName will be read using read_config_file, otherwise the contents of
% the variable itself are returned
read(VarName, Default) ->
    case application:get_env(VarName) of
        {ok, {file, ConfigFile}} ->
            read_config_file(ConfigFile);
        {ok, ConfigMap} ->
            ConfigMap;
        undefined ->
            Default
    end.


default_repo_config() ->
    #{version => 2, repos => [], keys => []}.


default_user_config() ->
    #{max_lease_time => 7200,
      fe_tcp_port => 4929,
      receiver_config => #{size => 1,
                           max_overflow => 1},
      receiver_worker_config =>
          #{executable_path => "/usr/bin/cvmfs_receiver"},
      log_level => <<"info">>}.

% Load the repository and user configuration
%
% Load the repository and user configuration from a configuration map.
% The output of the "cvmfs_config:read" function returns a configuration map
% that can be passed to this function.
load(Cfg) ->
    load(Cfg, fun(K) -> load_key(K) end).


% Load the repository and user configuration using a specific
% key loading function
load(Cfg, KeyLoader) ->
    RepoCfg = maps:get(repos, Cfg, []),
    KeyCfg = maps:get(keys, Cfg, []),

    CfgVer = maps:get(version, Cfg, 1),
    case CfgVer of
        1 ->
            Keys = lists:map(KeyLoader, KeyCfg),
            {ok, RepoCfg, Keys};
        _ ->
            {ok, Repos} = process_repos(RepoCfg),
            process_keys(KeyCfg, Repos, KeyLoader)
    end.


process_repos(RepoCfg) ->
    AddDefaultKeyId =
        fun(Name) when is_binary(Name) ->
                #{domain => Name, keys => default};
           (#{domain := _Name, keys := _Keys} = R) ->
                R
        end,
    {ok, lists:map(AddDefaultKeyId, RepoCfg)}.


process_keys(KeyCfg, Repos, KeyLoader) ->
    try lists:map(KeyLoader, KeyCfg) of
        SpecifiedKeys ->
            ProcessDefaultKey = fun(R, {AccRepos, AccKeys}) ->
                #{domain := Name, keys := K} = R,
                case K of
                    default ->
                        try KeyLoader(#{type => <<"file">>,
                                        file_name => <<"/etc/cvmfs/keys/", Name/binary, ".gw">>}) of
                            Key ->
                                #{id := KeyId} = Key,
                                case lists:search(fun(#{id := Id}) -> Id =:= KeyId end,
                                                  SpecifiedKeys) of
                                    false ->
                                        {[maps:put(keys, [#{id => KeyId, path => <<"/">>}], R) | AccRepos],
                                         [Key | AccKeys]};
                                    _ ->
                                        {[R | AccRepos], AccKeys}
                                end
                        catch throw:Reason -> {error, Reason}
                    end;
                    _ ->
                        {[R | AccRepos], AccKeys}
                end
            end,

            {UpdatedRepos, DefaultKeys} = lists:foldl(ProcessDefaultKey, {[], []}, Repos),
            {ok, UpdatedRepos, SpecifiedKeys ++ DefaultKeys}
    catch
        throw:Reason -> {error, Reason}
    end.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%    Private functions    %%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



read_config_file(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            jsx:decode(Data, [{labels, atom}, return_maps]);
        {error, Reason} ->
            {error, Reason}
    end.


load_key(#{type := <<"plain_text">>, id := Id, secret := Secret, repo_subpath := Path}) ->
    #{id => Id, secret => Secret, path => Path};
load_key(#{type := <<"plain_text">>, id := Id, secret := Secret}) ->
    #{id => Id, secret => Secret};
load_key(#{type := <<"file">>, file_name := FileName, repo_subpath := Path}) ->
    case cvmfs_keys:parse_file(FileName) of
        {ok, <<"plain_text">>, I, S} ->
            #{id => I, secret => S, path => Path};
        {error, Reason} ->
            throw(Reason)
    end;
load_key(#{type := <<"file">>, file_name := FileName}) ->
    case cvmfs_keys:parse_file(FileName) of
        {ok, <<"plain_text">>, I, S} ->
            #{id => I, secret => S};
        {error, Reason} ->
            throw(Reason)
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%    Tests    %%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%



process_repos_adds_default_keys_test() ->
    RepoCfg = [
        #{domain => <<"test1.domain.org">>,
          keys => [#{id => <<"testkey1">>, path => <<"/">>}]},
        #{domain => <<"test2.domain.org">>,
          keys => [#{id => <<"testkey1">>, path => <<"/">>},
                   #{id => <<"testkey2">>, path => <<"/">>}]},
        <<"test3.domain.org">>
    ],

    {ok, Repos} = process_repos(RepoCfg),
    {value, #{domain := Name, keys := KeyIds}} = lists:search(
        fun(#{domain := D}) -> D =:= <<"test3.domain.org">> end, Repos),
    ?assert(Name =:= <<"test3.domain.org">>),
    ?assert(KeyIds =:= default).

process_keys_adds_missing_key_definition_test() ->
    RepoCfg = [
        #{domain => <<"test1.domain.org">>,
          keys => [#{id => <<"testkey1">>, path => <<"/">>}]},
        #{domain => <<"test2.domain.org">>,
          keys => [#{id => <<"testkey1">>, path => <<"/">>},
                   #{id => <<"testkey2">>, path => <<"/">>}]},
        #{domain => <<"test3.domain.org">>,
          keys => default}
    ],
    KeyCfg = [
        #{type => <<"plain_text">>,
          id => <<"testkey1">>,
          secret => <<"SECRET1">>},
        #{type => <<"plain_text">>,
          id => <<"testkey2">>,
          secret => <<"SECRET2">>}
    ],

    {ok, Repos, Keys} = process_keys(KeyCfg, RepoCfg, fun(K) -> mock_load_key(K) end),

    {value, #{secret := Secret}} = lists:search(
        fun(#{id := Id}) -> Id =:= <<"test3.domain.org">> end, Keys),
    ?assert(Secret =:= <<"mocksecret">>),

    {value, #{keys := KeyIds}} = lists:search(
        fun(#{domain := D}) -> D =:= <<"test3.domain.org">> end, Repos),
    ?assert(KeyIds =:= [#{id => <<"test3.domain.org">>, path => <<"/">>}]).


load_with_empty_config_test() ->
    {ok, _, _} = load(#{}, fun(K) -> load_key(K) end),
    {ok, _, _} = load(#{version => 1}, fun(K) -> load_key(K) end),
    {ok, _, _} = load(#{version => 2}, fun(K) -> load_key(K) end).

mock_load_key(#{type := <<"plain_text">>, id := Id, secret := Secret}) ->
    #{id => Id, secret => Secret};
mock_load_key(#{type := <<"file">>, file_name := FileName}) ->
    [Id | _] = binary:split(lists:last(binary:split(FileName, <<"/">>, [global])), <<".gw">>),
    #{id => Id, secret => <<"mocksecret">>}.
