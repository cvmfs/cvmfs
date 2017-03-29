%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_services top level supervisor.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_services_sup).

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
init({EnabledWorkers, Repos, Keys, PoolConfig}) ->
    SupervisorSpecs = #{strategy => one_for_all,
                        intensity => 5,
                        period => 5},
    ReceiverPoolConfig = lists:append(PoolConfig, [{name, {local, cvmfs_receiver_pool}},
                                                   {worker_module, cvmfs_receiver}]),
    WorkerSpecs = #{
      cvmfs_auth => #{id => cvmfs_auth,
                      start => {cvmfs_auth, start_link, [{Repos, Keys}]},
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
      cvmfs_fe => #{id => cvmfs_fe,
                    start => {cvmfs_fe, start_link, []},
                    restart => permanent,
                    shutdown => 2000,
                    type => supervisor,
                    modules => [cvmfs_fe]},
      cvmfs_receiver_pool => poolboy:child_spec(cvmfs_receiver_pool, ReceiverPoolConfig, [])
     },
    {ok, {SupervisorSpecs, lists:foldr(fun(W, Acc) -> [maps:get(W, WorkerSpecs) | Acc] end,
                                       [],
                                       EnabledWorkers)}}.


%%====================================================================
%% Internal functions
%%====================================================================
