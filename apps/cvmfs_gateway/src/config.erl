%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to configuration
%%% @end
%%%-------------------------------------------------------------------

-module(config).

-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").

-export([read/2,
         default_repo_config/0,
         default_user_config/0,
         load/1,
         load/2]).


read(VarName, Defaults) ->
    case application:get_env(VarName) of
        {ok, {file, ConfigFile}} ->
            read_config_file(ConfigFile);
        {ok, ConfigMap} ->
            ConfigMap;
        undefined ->
            Defaults
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


load(Cfg) ->
    load(Cfg, fun(K) -> load_key(K) end).


load(Cfg, KeyLoader) ->
    RepoCfg = maps:get(repos, Cfg),
    KeyCfg = maps:get(keys, Cfg),

    CfgVer = maps:get(version, Cfg, 1),
    case CfgVer of
        1 ->
            Keys = lists:map(KeyLoader, KeyCfg),
            {ok, RepoCfg, Keys};
        _ ->
            {ok, Repos} = load_repos(RepoCfg),
            {ok, UpdatedRepos, Keys} = load_keys(KeyCfg, Repos, KeyLoader),
            {ok, UpdatedRepos, Keys}
    end.


load_repos(RepoCfg) ->
    AddDefaultKeyId =
        fun(Name) when is_binary(Name) ->
                #{domain => Name, keys => default};
           (#{domain := _Name, keys := _Keys} = R) ->
                R
        end,
    {ok, lists:map(AddDefaultKeyId, RepoCfg)}.


load_keys(KeyCfg, Repos, KeyLoader) ->
    SpecifiedKeys = lists:map(KeyLoader, KeyCfg),

    ProcessDefaultKey = fun(R, {AccRepos, AccKeys}) ->
        #{domain := Name, keys := K} = R,
        case K of
            default ->
                Key = KeyLoader(#{type => <<"file">>,
                                  file_name => <<"/etc/cvmfs/keys/", Name/binary, ".gw">>,
                                  repo_subpath => <<"/">>}),
                #{id := KeyId} = Key,
                case lists:search(fun(#{id := Id}) -> Id =:= KeyId end, SpecifiedKeys) of
                    false ->
                        {[#{domain => Name, keys => [KeyId]} | AccRepos],
                         [Key | AccKeys]};
                    _ ->
                        {[R | AccRepos], AccKeys}
                end;
            _ ->
                {[R | AccRepos], AccKeys}
        end
    end,

    {UpdatedRepos, DefaultKeys} = lists:foldl(ProcessDefaultKey, {[], []}, Repos),
    {ok, UpdatedRepos, SpecifiedKeys ++ DefaultKeys}.


%%% Private functions

read_config_file(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            jsx:decode(Data, [{labels, atom}, return_maps]);
        {error, Reason} ->
            {error, Reason}
    end.


load_key(#{type := <<"plain_text">>, id := Id, secret := Secret, repo_subpath := Path}) ->
    #{id => Id, secret => Secret, path => Path};
load_key(#{type := <<"file">>, file_name := FileName, repo_subpath := Path}) ->
    {<<"plain_text">>, I, S} = keys:parse_file(FileName),
    #{id => I, secret => S, path => Path}.


get_repo_names(Repos) ->
    lists:map(fun(#{domain := Name}) -> Name end, Repos).


%%% Tests

load_repos_adds_default_keys_test() ->
    RepoCfg = [
        #{domain => <<"test1.domain.org">>,
        keys => [<<"testkey1">>]},
        #{domain => <<"test2.domain.org">>,
        keys => [<<"testkey1">>, <<"testkey2">>]},
        <<"test3.domain.org">>
    ],

    {ok, Repos} = load_repos(RepoCfg),
    {value, #{domain := Name, keys := KeyIds}} = lists:search(
        fun(#{domain := D}) -> D =:= <<"test3.domain.org">> end, Repos),
    ?assert(Name =:= <<"test3.domain.org">>),
    ?assert(KeyIds =:= default),

    ok.

load_keys_adds_missing_key_definition_test() ->
    RepoCfg = [
        #{domain => <<"test1.domain.org">>,
          keys => [<<"testkey1">>]},
        #{domain => <<"test2.domain.org">>,
          keys => [<<"testkey1">>, <<"testkey2">>]},
        #{domain => <<"test3.domain.org">>,
          keys => default}
    ],
    KeyCfg = [
        #{type => <<"plain_text">>,
          id => <<"testkey1">>,
          secret => <<"SECRET1">>,
          repo_subpath => <<"/">>},
        #{type => <<"plain_text">>,
          id => <<"testkey2">>,
          secret => <<"SECRET2">>,
          repo_subpath => <<"/path">>}
    ],

    {ok, Repos, Keys} = load_keys(KeyCfg, RepoCfg, fun(K) -> mock_load_key(K) end),

    {value, #{secret := Secret}} = lists:search(
        fun(#{id := Id}) -> Id =:= <<"test3.domain.org">> end, Keys),
    ?assert(Secret =:= <<"mocksecret">>).

mock_load_key(#{type := <<"plain_text">>, id := Id, secret := Secret, repo_subpath := Path}) ->
    #{id => Id, secret => Secret, path => Path};
mock_load_key(#{type := <<"file">>, file_name := FileName, repo_subpath := Path}) ->
    [Id | _] = binary:split(lists:last(binary:split(FileName, <<"/">>, [global])), <<".gw">>),
    #{id => Id, secret => <<"mocksecret">>, path => Path}.
