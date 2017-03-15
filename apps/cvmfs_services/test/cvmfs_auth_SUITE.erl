%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([add_repo/1, remove_repo/1
        ,add_key/1, remove_key/1
        ,list_repos/1
        ,valid_keyid_valid_paths/1
        ,invalid_keyid_error/1
        ,valid_keyid_invalid_paths/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [{group, queries}
    ,{group, repo_operations}
    ,{group, key_operations}].

groups() ->
    [{queries, [parallel], [list_repos
                           ,valid_keyid_valid_paths
                           ,invalid_keyid_error
                           ,valid_keyid_invalid_paths]}
    ,{repo_operations, [], [add_repo, remove_repo]}
    ,{key_operations, [], [add_key, remove_key]}].


%% Set up, tear down

init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    ok = application:load(cvmfs_services),
    ok = ct:require(repos),
    ok = ct:require(keys),
    ok = application:set_env(cvmfs_services, enabled_services, [cvmfs_auth]),
    ok = application:set_env(cvmfs_services, repo_config, #{repos => ct:get_config(repos)
                                                           ,keys => ct:get_config(keys)}),
    {ok, _} = application:ensure_all_started(cvmfs_services),
    Config.

end_per_suite(_Config) ->
    application:stop(cvmfs_services),
    application:unload(cvmfs_services),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, _Config) ->
    [].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Test cases follow

list_repos(_Config) ->
    Repos1 = lists:sort(cvmfs_auth:get_repos()),
    Repos2 = lists:sort(lists:foldl(fun({N, _, _}, Acc) -> [N | Acc] end, [], ct:get_config(repos))),
    Repos1 = Repos2.

valid_keyid_valid_paths(_Config) ->
    {ok, true} = cvmfs_auth:check_keyid_for_repo(<<"key1">>, <<"repo1">>).

invalid_keyid_error(_Config) ->
    {ok, false} = cvmfs_auth:check_keyid_for_repo(<<"key2">>, <<"repo1">>).

valid_keyid_invalid_paths(_Config) ->
    {error, invalid_repo} = cvmfs_auth:check_keyid_for_repo(<<"key1">>, <<"bad_repo">>).

add_repo(_Config) ->
    ok = cvmfs_auth:add_repo(<<"new_repo">>, <<"/new/repo/path">>, [<<"key">>]),
    true = lists:member(<<"new_repo">>, cvmfs_auth:get_repos()).

remove_repo(_Config) ->
    cvmfs_auth:remove_repo(<<"repo3">>),
    false = lists:member(<<"repo3">>, cvmfs_auth:get_repos()).

add_key(_Config) ->
    ok = cvmfs_auth:add_key(<<"new_key">>, <<"secret">>).

remove_key(_Config) ->
    ok = cvmfs_auth:remove_key(<<"key3">>).

