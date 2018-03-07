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
        ,reload_config/1
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
    ,{group, reload}
    ,{group, repo_operations}
    ,{group, key_operations}].

groups() ->
    [{queries, [], [valid_key_valid_path
                   ,valid_key_valid_subpath
                   ,invalid_key_error
                   ,valid_key_invalid_repo
                   ,valid_key_invalid_path]}
    ,{reload, [], [reload_config]}
    ,{repo_operations, [], [add_repo, remove_repo]}
    ,{key_operations, [], [add_key, remove_key]}].


%% Set up, tear down

init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    ok = application:load(cvmfs_gateway),
    ok = application:set_env(cvmfs_gateway, enabled_services, [cvmfs_auth]),
    ok = application:set_env(cvmfs_gateway, repo_config,
                             cvmfs_test_util:make_test_repo_config()),
    {ok, _} = application:ensure_all_started(cvmfs_gateway),
    Config.

end_per_suite(_Config) ->
    application:stop(cvmfs_gateway),
    application:unload(cvmfs_gateway),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, _Config) ->
    [].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Test cases follow

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
    true = lists:member({<<"/new/repo/path">>, [<<"key">>]}, cvmfs_auth:get_repos()).

remove_repo(_Config) ->
    cvmfs_auth:remove_repo(<<"repo3">>),
    false = lists:member(<<"repo3">>, cvmfs_auth:get_repos()).

add_key(_Config) ->
    ok = cvmfs_auth:add_key(<<"new_key">>, <<"secret">>, <<"/">>).

remove_key(_Config) ->
    ok = cvmfs_auth:remove_key(<<"key3">>).

reload_config(_Config) ->
    ok = cvmfs_auth:add_repo(<<"/new/repo/path">>, [<<"key">>]),
    true = lists:member({<<"/new/repo/path">>, [<<"key">>]}, cvmfs_auth:get_repos()),
    ok = cvmfs_auth:reload_repo_config(),
    false = lists:member({<<"/new/repo/path">>, [<<"key">>]}, cvmfs_auth:get_repos()).

