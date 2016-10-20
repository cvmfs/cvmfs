%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_lease_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    {ok, MnesiaSchema} = application:get_env(cvmfs_services, mnesia_schema),
    cvmfs_lease_sup:start_link(MnesiaSchema).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
