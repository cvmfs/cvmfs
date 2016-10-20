%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_router top level supervisor.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_router).

-export([start_link/0]).

start_link() ->
    Dispatch = cowboy_router:compile([{'_', [{"/", cvmfs_fe_test_handler, []}]}]),
    {ok, Pid} = cowboy:start_clear(cvmfs_fe_http_listener, 100, [{port, 8080}],
                                   #{env => #{dispatch => Dispatch}}),
    Pid.
