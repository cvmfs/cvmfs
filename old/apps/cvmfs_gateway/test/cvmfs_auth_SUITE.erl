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
-export([valid_key_valid_path/1
        ,valid_key_valid_subpath/1
        ,invalid_key_error/1
        ,valid_key_invalid_repo/1
        ,valid_key_invalid_path/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [{group, queries}].

groups() ->
    [{queries, [], [valid_key_valid_path
                   ,valid_key_valid_subpath
                   ,invalid_key_error
                   ,valid_key_invalid_repo
                   ,valid_key_invalid_path]}].


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
