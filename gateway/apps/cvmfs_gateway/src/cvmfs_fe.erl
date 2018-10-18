%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe - CVMFS repository gateway HTTP front-end
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe).

-export([start_link/1]).


%%--------------------------------------------------------------------
%% @doc
%% Starts the front-end HTTP listener process.
%%
%% @end
%%--------------------------------------------------------------------
start_link([TcpPort]) ->
    %% Compile a routing table, for dispatching requests for different resources
    %% to a set of handler modules. The handler modules implement the init/2 callback
    %% required by Cowboy
    Dispatch = cowboy_router:compile([{'_', [
                                             {cvmfs_version:api_root(), cvmfs_root_handler, []},
                                             {cvmfs_version:api_root() ++ "/repos/[:id]", cvmfs_repos_handler, []},
                                             {cvmfs_version:api_root() ++ "/leases/[:token]", cvmfs_leases_handler, []},
                                             {cvmfs_version:api_root() ++ "/payloads/[:id]", cvmfs_payloads_handler, []}
                                            ]}]),
    %% Start the HTTP listener process configured with the routing table
    cowboy:start_clear(cvmfs_fe,
                       [{port, TcpPort}],
                       #{env => #{dispatch => Dispatch},
                         idle_timeout => cvmfs_app_util:get_max_lease_time(),
                         inactivity_timeout => cvmfs_app_util:get_max_lease_time()}).

