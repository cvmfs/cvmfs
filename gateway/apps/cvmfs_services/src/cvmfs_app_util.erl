%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_app_utl public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_app_util).

-export([get_max_lease_time/1]).

get_max_lease_time(sec) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_services, max_lease_time),
    MaxLeaseTime;
get_max_lease_time(ms) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_services, max_lease_time),
    MaxLeaseTime * 1000.

