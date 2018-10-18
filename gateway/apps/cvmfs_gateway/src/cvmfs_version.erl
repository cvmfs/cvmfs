%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_version
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_version).

-export([api_version/0, min_api_version/0, api_root/0]).


-define(API_VERSION, 1).
-define(API_ROOT, "/api/v" ++ integer_to_list(?API_VERSION)).


-spec api_version() -> integer().
api_version() ->
    ?API_VERSION.


-spec min_api_version() -> integer().
min_api_version() ->
    ?API_VERSION.


-spec api_root() -> list().
api_root() ->
    "/api/v" ++ integer_to_list(api_version()).

