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
        ,add_user/1, remove_user/1
        ,list_repos/1, list_users/1
        ,valid_username_valid_paths/1
        ,valid_username_invalid_paths/1
        ,valid_username_no_paths/1
        ,invalid_username_error/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [{group, queries}
    ,{group, repo_operations}
    ,{group, user_operations}].

groups() ->
    [{queries, [parallel], [list_repos
                   ,valid_username_valid_paths
                   ,valid_username_invalid_paths
                   ,valid_username_no_paths
                   ,invalid_username_error]}
    ,{repo_operations, [], [add_repo, remove_repo]}
    ,{user_operations, [], [add_user, remove_user]}].


%% Set up, tear down

init_per_suite(Config) ->
    application:start(mnesia),
    ok = ct:require(repos),
    ok = ct:require(acl),
    Watcher = spawn(fun() ->
                            {ok, _} = cvmfs_auth:start_link({ct:get_config(repos),
                                                             ct:get_config(acl)}),
                            receive
                                test_suite_end ->
                                    cvmfs_auth:stop()
                            end
                    end),
    [{watcher_process, Watcher} | Config].

end_per_suite(Config) ->
    Watcher = ?config(watcher_process, Config),
    Watcher ! test_suite_end,
    application:stop(mnesia),
    ok.

init_per_testcase(_TestCase, _Config) ->
    [].

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Test cases follow

list_repos(_Config) ->
    Repos1 = lists:sort(cvmfs_auth:get_repos()),
    Repos2 = lists:sort(lists:foldl(fun({N, _}, Acc) -> [N | Acc] end, [], ct:get_config(repos))),
    lists:zipwith(fun(A, B) -> A = B end, Repos1, Repos2).

list_users(_Config) ->
    Users1 = lists:sort(cvmfs_auth:get_users()),
    Users2 = lists:sort(lists:foldl(fun({U, _}, Acc) -> [U | Acc] end, [], ct:get_config(acl))),
    lists:zipwith(fun(A, B) -> A = B end, Users1, Users2).

valid_username_valid_paths(_Config) ->
    {ok, Results} = cvmfs_auth:get_user_permissions(<<"user1">>),
    Results = [<<"/path/to/repo/1">>, <<"/path/to/another/repo">>, <<"/path/to/last/repo">>].

valid_username_invalid_paths(_Config) ->
    {ok, []} = cvmfs_auth:get_user_permissions(<<"user2">>).

valid_username_no_paths(_Config) ->
    {ok, []} = cvmfs_auth:get_user_permissions(<<"user3">>).

invalid_username_error(_Config) ->
    user_not_found = cvmfs_auth:get_user_permissions(<<"not_a_username">>).

add_repo(_Config) ->
    ok = cvmfs_auth:add_repo(<<"new_repo">>, <<"/new/repo/path">>),
    true = lists:member(<<"new_repo">>, cvmfs_auth:get_repos()).

remove_repo(_Config) ->
    cvmfs_auth:remove_repo(<<"repo3">>),
    false = lists:member(<<"repo3">>, cvmfs_auth:get_repos()).

add_user(_Config) ->
    ok = cvmfs_auth:add_user(<<"new_user">>, [<<"repo1">>]),
    true = lists:member(<<"new_user">>, cvmfs_auth:get_users()).

remove_user(_Config) ->
    cvmfs_auth:remove_user(<<"user3">>),
    false = lists:member(<<"user3">>, cvmfs_auth:get_users()).

