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
                                            ,{"/api/user/[:id]", cvmfs_fe_user_handler, []}
                                            ,{"/api/repo/[:id]", cvmfs_fe_repo_handler, []}
                                            ]}]),
    cowboy:start_clear(cvmfs_fe_http_listener, 100,
                       [{port, 8080}],
                       #{env => #{dispatch => Dispatch}}).
