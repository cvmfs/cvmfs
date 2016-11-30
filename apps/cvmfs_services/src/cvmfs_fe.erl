%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc front-end
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(fe).

-export([start_link/0]).

start_link() ->
    Dispatch = cowboy_router:compile([{'_', [
                                             {"/api", root_handler, []},
                                             {"/api/users/[:id]", users_handler, []},
                                             {"/api/repos/[:id]", repos_handler, []},
                                             {"/api/leases/[:id]", leases_handler, []}
                                            ]}]),
    cowboy:start_clear(fe, 100,
                       [{port, 8080}],
                       #{env => #{dispatch => Dispatch}}).
