%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to configuration
%%% @end
%%%-------------------------------------------------------------------

-module(config).

-compile([{parse_transform, lager_transform}]).

-export([read/2,
         default_repo_config/0,
         default_user_config/0,
         get_repo_names/1,
         load_repos/1,
         load_keys/2,
         load_keys/3]).

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


get_repo_names(Repos) ->
    Extract = fun(#{domain := RepoName}) ->
                    RepoName;
                 (RepoName) when is_binary(RepoName) ->
                    RepoName
              end,
    lists:map(Extract, Repos).


-spec load_repos(RepoCfg) -> FixedRepoCfg
    when RepoCfg :: [binary() | #{atom() => binary() | [binary()]}],
         FixedRepoCfg :: [#{atom() => binary() | [binary()]}].
load_repos(RepoCfg) ->
    AddDefaultKeyId =
        fun(Name) when is_binary(Name) ->
                #{domain => Name, keys => [Name]};
           (#{domain := _Name, keys := _Keys} = R) ->
                R
        end,
    lists:map(AddDefaultKeyId, RepoCfg).


load_keys(KeyCfg, Repos) ->
    load_keys(KeyCfg, Repos, fun(K) -> load_key(K) end).


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

    DefinedKeys ++ lists:map(KeyLoader, DefaultKeys).


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