%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_gateway top level supervisor.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_gateway_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Args) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Args).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init({EnabledWorkers, PoolConfig, WorkerConfig}) ->
    SupervisorSpecs = #{strategy => one_for_all,
                        intensity => 5,
                        period => 5},
    ReceiverPoolConfig = [{name, {local, cvmfs_receiver_pool}} | PoolConfig],
    %% We create a duplicate of the receiver worker pool configuration, used
    %% for a second worker pool, which services "fast" requests - generating
    %% and checking session tokens. For the moment, the size of the pool is
    %% fixed to 1, since the requests complete almost instantaneously.
    FastReceiverPoolConfig = [{name,
                               {local, cvmfs_fast_receiver_pool}} |
                              lists:keystore(size, 1, PoolConfig, {size, 1})],
    WorkerSpecs = #{
      cvmfs_auth => #{id => cvmfs_auth,
                      start => {cvmfs_auth, start_link, []},
                      restart => permanent,
                      shutdown => 2000,
                      type => worker,
                      modules => [cvmfs_auth]},
      cvmfs_be => #{id => cvmfs_be,
                    start => {cvmfs_be, start_link, [{}]},
                    restart => permanent,
                    shutdown => 2000,
                    type => worker,
                    modules => [cvmfs_be]},
      cvmfs_lease => #{id => cvmfs_lease,
                       start => {cvmfs_lease, start_link, [{}]},
                       restart => permanent,
                       shutdown => 2000,
                       type => worker,
                       modules => [cvmfs_lease]},
      %% Two receiver worker pools are created: one for servicing longer requests, like payload
      %% submission and committing transactions, and another one for fast requests - generating
      %% and checking session tokens. This ensures that a client requesting a new lease can't
      %% be blocked when all the workers in the normal pool are servicing long running requests.
      cvmfs_receiver_pool => poolboy:child_spec(cvmfs_receiver_pool, ReceiverPoolConfig, WorkerConfig),
      cvmfs_fast_receiver_pool => poolboy:child_spec(cvmfs_fast_receiver_pool,
                                                     FastReceiverPoolConfig,
                                                     WorkerConfig),
      cvmfs_commit_sup => #{id => cvmfs_commit_sup,
                            start => {cvmfs_commit_sup, start_link, []},
                            restart => permanent,
                            shutdown => infinity,
                            type => supervisor,
                            modules => [cvmfs_commit_sup]}
     },
    {ok, {SupervisorSpecs, lists:foldr(fun(W, Acc) -> [maps:get(W, WorkerSpecs) | Acc] end,
                                       [],
                                       EnabledWorkers)}}.


%%====================================================================
%% Internal functions
%%====================================================================
