%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_router
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_router).

-export([start_link/0]).

start_link() ->
    Dispatch = cowboy_router:compile([{'_', [
                                             {"/api", cvmfs_fe_root_handler, []}
                                            ,{"/api/users/[:id]", cvmfs_fe_users_handler, []}
                                            ,{"/api/repos/[:id]", cvmfs_fe_repos_handler, []}
                                            ,{"/api/leases/[:id]", cvmfs_fe_sessions_handler, []}
                                            ]}]),
    cowboy:start_clear(cvmfs_fe_http_listener, 100,
                       [{port, 8080}],
                       #{env => #{dispatch => Dispatch}}).
