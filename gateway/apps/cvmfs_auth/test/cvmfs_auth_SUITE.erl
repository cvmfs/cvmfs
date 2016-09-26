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

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([get_user_permissions_test/1
        ,repo_operations_test/1
        ,user_operations_test/1]).

%% Note: These tests are for the API of the main cvmfs_auth module and
%% since they exercise multiple other modules, they should become
%% integration tests. Should move them to common test (ct), which is
%% better suited for the testing of stateful systems.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [get_user_permissions_test
    ,repo_operations_test
    ,user_operations_test].

init_per_testcase(_TestCaseName, _Config) ->
    application:start(mnesia),
    ok = ct:require(repos),
    ok = ct:require(acl),
    {ok, _} = cvmfs_auth:start_link({ct:get_config({repos})
                                    ,ct:get_config({acl})}),
    [].

end_per_testcase(_TestCaseName, _Config) ->
    application:stop(mnesia),
    ok.

get_user_permissions_test(_Config) ->
    %% Valid username returns valid paths
    {ok, Results} = cvmfs_auth:get_user_permissions(<<"user1">>),
    Results = [<<"/path/to/repo/1">>, <<"/path/to/another/repo">>, <<"/path/to/last/repo">>],

    %% Valid username can have invalid paths, should return nothing
    {ok, []} = cvmfs_auth:get_user_permissions(<<"user2">>),

    %% Valid username can have no paths
    {ok, []} = cvmfs_auth:get_user_permissions(<<"user3">>),

    %% Invalid username returns error
    user_not_found = cvmfs_auth:get_user_permissions(<<"not_a_username">>).

repo_operations_test(_Config) ->
    %% Repo operations work
    ok = cvmfs_auth:add_repo(<<"new_repo">>, <<"/new/repo/path">>),
    true = lists:member(<<"new_repo">>, cvmfs_auth:get_repos()),
    cvmfs_auth:remove_repo(<<"new_repo">>),
    false = lists:member(<<"new_repo">>, cvmfs_auth:get_repos()).

user_operations_test(_Config) ->
    %% User operations work
    ok = cvmfs_auth:add_user(<<"new_user">>, [<<"repo1">>]),
    true = lists:member(<<"new_user">>, cvmfs_auth:get_users()),
    cvmfs_auth:remove_user(<<"new_user">>),
    false = lists:member(<<"new_user">>, cvmfs_auth:get_users()).

