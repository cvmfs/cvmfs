%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_version
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_version).

-export([api_protocol_version/0, min_api_protocol_version/0, api_root/0]).


-define(API_PROTOCOL_VERSION, 2).
-define(API_ROOT, "/api/v1").


-spec api_protocol_version() -> integer().
api_protocol_version() ->
    ?API_PROTOCOL_VERSION.


-spec min_api_protocol_version() -> integer().
min_api_protocol_version() ->
    ?API_PROTOCOL_VERSION.


-spec api_root() -> list().
api_root() ->
    ?API_ROOT.

