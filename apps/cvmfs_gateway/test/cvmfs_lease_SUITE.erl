%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0
        ,init_per_suite/1, end_per_suite/1
        ,init_per_testcase/2, end_per_testcase/2]).

-export([new_lease/1, new_lease_busy/1, new_lease_expired/1
        ,new_lease_conflict/1, remove_lease_existing/1
        ,remove_lease_nonexisting/1, check_lease_valid/1
        ,check_lease_expired/1, check_invalid_lease/1
        ,get_lease_path_valid/1, get_lease_path_expired/1
        ,get_lease_path_invalid/1, clear_leases/1]).


%% Tests description

all() ->
    [{group, new_leases}
    ,{group, end_leases}
    ,{group, check_leases}
    ,{group, get_lease_paths}].

groups() ->
    [{new_leases, [], [new_lease
                      ,new_lease_busy
                      ,new_lease_expired
                      ,new_lease_conflict]}
    ,{end_leases, [], [remove_lease_existing
                      ,remove_lease_nonexisting
                      ,clear_leases]}
    ,{check_leases, [], [check_lease_valid
                        ,check_lease_expired
                        ,check_invalid_lease]}
    ,{get_lease_paths, [], [get_lease_path_valid
                           ,get_lease_path_expired
                           ,get_lease_path_invalid]}].

%% Set up, tear down

init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    ok = application:load(cvmfs_gateway),
    ok = application:set_env(cvmfs_gateway, enabled_services, [cvmfs_lease]),

    MaxLeaseTime = 1, % seconds
    TestUserVars = cvmfs_test_util:make_test_user_vars(MaxLeaseTime),
    ok = application:set_env(cvmfs_gateway, user_config, TestUserVars),

    {ok, _} = application:ensure_all_started(cvmfs_gateway),

    lists:flatten([{max_lease_time, MaxLeaseTime}, Config]).

end_per_suite(_Config) ->
    application:stop(cvmfs_gateway),
    application:unload(cvmfs_gateway),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, Config) ->
    cvmfs_lease:clear_leases(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.


%% Test cases

new_lease(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    [{lease, {P, <<>>}, U, Public, Secret, _}] = cvmfs_lease:get_leases().


new_lease_busy(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    {busy, _} = cvmfs_lease:request_lease(U, P, Public, Secret).

new_lease_expired(Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    SleepTime = ?config(max_lease_time, Config) * 1000 + 10,
    ct:sleep(SleepTime),
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    [{lease, {P, <<>>}, U, Public, Secret, _}] = cvmfs_lease:get_leases().

new_lease_conflict(_Config) ->
    U = <<"user">>,
    P1 = <<"path">>,
    P2 = <<"path/below/the/first/one">>,
    Public1 = <<"public1">>,
    Public2 = <<"public2">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P1, Public1, Secret),
    {busy, _} = cvmfs_lease:request_lease(U, P2, Public2, Secret),
    ok = cvmfs_lease:end_lease(Public1),
    ok = cvmfs_lease:request_lease(U, P2, Public2, Secret),
    {busy, _} = cvmfs_lease:request_lease(U, P1, Public1, Secret).

remove_lease_existing(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    ok = cvmfs_lease:end_lease(Public).

remove_lease_nonexisting(_Config) ->
    P = <<"path">>,
    ok = cvmfs_lease:end_lease(P).

clear_leases(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    [{lease, {P, <<>>}, U, Public, Secret, _}] = cvmfs_lease:get_leases(),
    ok = cvmfs_lease:clear_leases(),
    [] = cvmfs_lease:get_leases().

check_lease_valid(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    {ok, Secret} = cvmfs_lease:get_lease_secret(Public).

check_lease_expired(Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    SleepTime = ?config(max_lease_time, Config) * 1000 + 10,
    ct:sleep(SleepTime),
    {error, lease_expired} = cvmfs_lease:get_lease_secret(Public).

check_invalid_lease(_Config) ->
    Public = <<"public">>,
    {error, invalid_lease} = cvmfs_lease:get_lease_secret(Public).

get_lease_path_valid(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    {ok, <<"path/">>} = cvmfs_lease:get_lease_path(Public).

get_lease_path_expired(Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    SleepTime = ?config(max_lease_time, Config) * 1000 + 10,
    ct:sleep(SleepTime),
    {error, lease_expired} = cvmfs_lease:get_lease_path(Public).

get_lease_path_invalid(_Config) ->
    U = <<"user">>,
    P = <<"path">>,
    Public = <<"public">>,
    Secret = <<"secret">>,
    ok = cvmfs_lease:request_lease(U, P, Public, Secret),
    {error, invalid_lease} = cvmfs_lease:get_lease_path(<<"invalid_user">>).
