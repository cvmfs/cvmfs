%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_app_utl public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_app_util).

-export([get_max_lease_time/0]).

get_max_lease_time() ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_gateway, max_lease_time),
    MaxLeaseTime.
