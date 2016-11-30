%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe).

-export([start_link/0]).

start_link() ->
    Dispatch = cowboy_router:compile([{'_', [
                                             {"/api", cvmfs_root_handler, []},
                                             {"/api/users/[:id]", cvmfs_users_handler, []},
                                             {"/api/repos/[:id]", cvmfs_repos_handler, []},
                                             {"/api/leases/[:id]", cvmfs_leases_handler, []},
                                             {"/api/payloads/[:id]", cvmfs_payloads_handler, []}
                                            ]}]),
    cowboy:start_clear(cvmfs_fe, 100,
                       [{port, 8080}],
                       #{env => #{dispatch => Dispatch}}).
