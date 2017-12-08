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
        ,valid_key_valid_path/1
        ,valid_key_valid_subpath/1
        ,invalid_key_error/1
        ,valid_key_invalid_repo/1
        ,valid_key_invalid_path/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [{group, queries}
    ,{group, repo_operations}
    ,{group, key_operations}].

groups() ->
    [{queries, [parallel], [list_repos
                           ,valid_key_valid_path
                           ,valid_key_valid_subpath
                           ,invalid_key_error
                           ,valid_key_invalid_repo
                           ,valid_key_invalid_path]}
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
    Repos2 = lists:sort(lists:foldl(fun({N, _}, Acc) -> [N | Acc] end, [], ct:get_config(repos))),
    Repos1 = Repos2.

valid_key_valid_path(_Config) ->
    ok = cvmfs_auth:check_key_for_repo_path(<<"key1">>, <<"repo1.domain1.org">>, <<"/">>).

valid_key_valid_subpath(_Config) ->
    ok = cvmfs_auth:check_key_for_repo_path(<<"key2">>, <<"repo3.domain3.org">>,
                                            <<"/subpath/dir">>).

invalid_key_error(_Config) ->
    {error, invalid_key} = cvmfs_auth:check_key_for_repo_path(<<"key2">>, <<"repo1.domain1.org">>, <<"/">>).

valid_key_invalid_repo(_Config) ->
    {error, invalid_repo} = cvmfs_auth:check_key_for_repo_path(<<"key1">>, <<"bad_repo">>, <<"/">>).

valid_key_invalid_path(_Config) ->
    {error, invalid_path} = cvmfs_auth:check_key_for_repo_path(<<"key2">>, <<"repo3.domain3.org">>,
                                                               <<"/forbidden_path">>).

add_repo(_Config) ->
    ok = cvmfs_auth:add_repo(<<"/new/repo/path">>, [<<"key">>]),
    true = lists:member(<<"/new/repo/path">>, cvmfs_auth:get_repos()).

remove_repo(_Config) ->
    cvmfs_auth:remove_repo(<<"repo3">>),
    false = lists:member(<<"repo3">>, cvmfs_auth:get_repos()).

add_key(_Config) ->
    ok = cvmfs_auth:add_key(<<"new_key">>, <<"secret">>, <<"/">>).

remove_key(_Config) ->
    ok = cvmfs_auth:remove_key(<<"key3">>).

