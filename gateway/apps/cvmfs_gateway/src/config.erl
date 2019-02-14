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
    #{repos => [], keys => []}.


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
    {ok, Repos} = load_repos(RepoCfg),
    {ok, UpdatedRepos, Keys} = load_keys(KeyCfg, Repos, KeyLoader),
    {ok, UpdatedRepos, Keys}.


-spec load_repos(RepoCfg) -> {ok, FixedRepoCfg}
    when RepoCfg :: [binary() | #{atom() => binary() | [binary()]}],
         FixedRepoCfg :: [#{atom() => binary() | [binary()]}].
load_repos(RepoCfg) ->
    AddDefaultKeyId =
        fun(Name) when is_binary(Name) ->
                #{domain => Name, keys => [Name]};
           (#{domain := _Name, keys := _Keys} = R) ->
                R
        end,
    {ok, lists:map(AddDefaultKeyId, RepoCfg)}.


load_keys(KeyCfg, Repos, KeyLoader) ->
    RepoNames = sets:from_list(get_repo_names(Repos)),

    ExtractKeyIds = fun(#{keys := Keys}) -> Keys end,
    ReferencedKeyIds = sets:from_list(lists:flatmap(ExtractKeyIds, Repos)),

    DefinedKeys = lists:map(KeyLoader, KeyCfg),
    DefinedKeyIds = sets:from_list(lists:map(fun(#{id := Id}) -> Id end, DefinedKeys)),

    ExtraKeyIds = sets:to_list(sets:subtract(ReferencedKeyIds, DefinedKeyIds)),

    AddIfDefaultKeyId = fun(Id, Acc) ->
        case sets:is_element(Id, RepoNames) of
            true ->
                [#{type => <<"file">>,
                   file_name => <<"/etc/cvmfs/keys/", Id/binary, ".gw">>,
                   repo_subpath => <<"/">>} | Acc];
            false ->
                lager:warning("Referenced key id: ~p has not been defined. "
                              ++ "It may not be possible to take leases on repositories "
                              ++ "configured with that key.", [Id]),
                Acc
            end
        end,
    DefaultKeys = lists:foldl(AddIfDefaultKeyId, [], ExtraKeyIds),

    {ok, Repos, DefinedKeys ++ lists:map(KeyLoader, DefaultKeys)}.


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
    #{domain := Name, keys := KeyIds} = lists:last(Repos),
    ?assert(Name =:= <<"test3.domain.org">>),
    ?assert(KeyIds =:= [<<"test3.domain.org">>]),

    ok.

load_keys_adds_missing_key_definition_test() ->
    Repos = [
        #{domain => <<"test1.domain.org">>,
          keys => [<<"testkey1">>]},
        #{domain => <<"test2.domain.org">>,
          keys => [<<"testkey1">>, <<"testkey2">>]},
        #{domain => <<"test3.domain.org">>,
          keys => [<<"test3.domain.org">>]}
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

    {ok, Repos, Keys} = load_keys(KeyCfg, Repos, fun(K) -> mock_load_key(K) end),

    #{secret := Secret} = lists:last(Keys),
    ?assert(Secret =:= <<"mocksecret">>).

mock_load_key(#{type := <<"plain_text">>, id := Id, secret := Secret, repo_subpath := Path}) ->
    #{id => Id, secret => Secret, path => Path};
mock_load_key(#{type := <<"file">>, file_name := FileName, repo_subpath := Path}) ->
    Id = lists:last(binary:split(FileName, <<"/">>, [global])),
    #{id => Id, secret => <<"mocksecret">>, path => Path}.
