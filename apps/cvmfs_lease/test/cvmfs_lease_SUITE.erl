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
        ,remove_lease_existing/1, remove_lease_nonexisting/1
        ,check_lease_valid/1, check_lease_expired/1, check_lease_not_found/1
        ,clear_leases/1]).


%% Tests description

all() ->
    [{group, new_leases}
    ,{group, end_leases}
    ,{group, check_leases}].

groups() ->
    [{new_leases, [], [new_lease
                      ,new_lease_busy
                      ,new_lease_expired]}
    ,{end_leases, [], [remove_lease_existing
                      ,remove_lease_nonexisting
                      ,clear_leases]}
    ,{check_leases, [], [check_lease_valid
                        ,check_lease_expired
                        ,check_lease_not_found]}].


%% Set up, tear down

init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:start(mnesia),

    MaxLeaseTime = 50, % milliseconds
    ok = application:load(cvmfs_lease),
    ok = application:set_env(cvmfs_lease, max_lease_time, MaxLeaseTime),
    {ok, _} = application:ensure_all_started(cvmfs_lease),
    lists:flatten([{max_lease_time, MaxLeaseTime}, Config]).

end_per_suite(_Config) ->
    application:stop(cvmfs_lease),
    application:unload(cvmfs_lease),
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
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    [{lease, {R, P}, U, _}] = cvmfs_lease:get_leases().


new_lease_busy(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    {busy, _} = cvmfs_lease:request_lease(U, R, P).

new_lease_expired(Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    SleepTime = ?config(max_lease_time, Config) + 10,
    ct:sleep(SleepTime),
    ok = cvmfs_lease:request_lease(U, R, P),
    [{lease, {R, P}, U, _}] = cvmfs_lease:get_leases().

remove_lease_existing(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    ok = cvmfs_lease:end_lease(R, P).

remove_lease_nonexisting(_Config) ->
    R = <<"repo">>,
    P = <<"path">>,
    {error, lease_not_found} = cvmfs_lease:end_lease(R, P).

clear_leases(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    [{lease, {R, P}, U, _}] = cvmfs_lease:get_leases(),
    ok = cvmfs_lease:clear_leases(),
    [] = cvmfs_lease:get_leases().

check_lease_valid(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    ok = cvmfs_lease:check_lease(U, R, P).

check_lease_expired(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    ok = cvmfs_lease:request_lease(U, R, P),
    SleepTime = 1500,
    ct:sleep(SleepTime),
    {error, lease_expired} = cvmfs_lease:check_lease(U, R, P).

check_lease_not_found(_Config) ->
    U = <<"user">>,
    R = <<"repo">>,
    P = <<"path">>,
    {error, lease_not_found} = cvmfs_lease:check_lease(U, R, P).

