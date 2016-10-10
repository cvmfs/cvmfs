%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_lease_app).

-compile([{parse_transform, lager_transform}]).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_lease, max_lease_time),
    {ok, MnesiaSchema} = application:get_env(cvmfs_services, mnesia_schema),
    cvmfs_lease_sup:start_link({MaxLeaseTime, MnesiaSchema}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
