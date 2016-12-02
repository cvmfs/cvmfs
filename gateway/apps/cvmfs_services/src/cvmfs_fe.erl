%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe - CVMFS repo services HTTP front-end
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe).

-export([start_link/0]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the front-end HTTP listener process.
%%
%% @end
%%--------------------------------------------------------------------
start_link() ->
    %% Compile a routing table, for dispatching requests for different resources
    %% to a set of handler modules. The handler modules implement the init/2 callback
    %% required by Cowboy
    Dispatch = cowboy_router:compile([{'_', [
                                             {"/api", cvmfs_root_handler, []},
                                             {"/api/users/[:id]", cvmfs_users_handler, []},
                                             {"/api/repos/[:id]", cvmfs_repos_handler, []},
                                             {"/api/leases/[:id]", cvmfs_leases_handler, []},
                                             {"/api/payloads/[:id]", cvmfs_payloads_handler, []}
                                            ]}]),
    %% Start the HTTP listener process configured with the routing table
    %% TODO: Port and other parameters should not be hard-coded and should moved to
    %%       the release configuration file
    cowboy:start_clear(cvmfs_fe, 100,
                       [{port, 8080}],
                       #{env => #{dispatch => Dispatch}}).
