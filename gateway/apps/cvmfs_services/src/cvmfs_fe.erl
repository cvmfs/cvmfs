%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe - CVMFS repo services HTTP front-end
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe).

-export([start_link/1, api_version/0]).

-define(API_VERSION, 1).
-define(API_ROOT, "/api/v" ++ integer_to_list(?API_VERSION)).

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
                                             {?API_ROOT, cvmfs_root_handler, []},
                                             {?API_ROOT ++ "/repos/[:id]", cvmfs_repos_handler, []},
                                             {?API_ROOT ++ "/leases/[:token]", cvmfs_leases_handler, []},
                                             {?API_ROOT ++ "/payloads/[:id]", cvmfs_payloads_handler, []}
                                            ]}]),
    %% Start the HTTP listener process configured with the routing table
    %% TODO: Port and other parameters should not be hard-coded and should moved to
    %%       the release configuration file
    cowboy:start_clear(cvmfs_fe, 100,
                       [{port, TcpPort}],
                       #{env => #{dispatch => Dispatch}}).


-spec api_version() -> integer().
api_version() ->
    ?API_VERSION.
