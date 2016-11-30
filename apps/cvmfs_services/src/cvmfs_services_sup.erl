%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease top level supervisor.
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
init(Args) ->
    SupervisorSpecs = #{strategy => one_for_all,
                        intensity => 5,
                        period => 5},
    AuthSpecs = #{id => auth,
                  start => {auth, start_link, [Args]},
                  restart => permanent,
                  shutdown => 2000,
                  type => worker,
                  modules => [auth]},
    BeSpecs = #{id => be,
                start => {be, start_link, [{}]},
                restart => permanent,
                shutdown => 2000,
                type => worker,
                modules => [be]},
    LeaseSpecs = #{id => lease,
                   start => {lease, start_link, [Args]},
                   restart => permanent,
                   shutdown => 2000,
                   type => worker,
                   modules => [lease]},
    FeSpecs = #{id => fe,
                start => {fe, start_link, []},
                restart => permanent,
                shutdown => 2000,
                type => supervisor,
                modules => [fe]},
    {ok, {SupervisorSpecs, [AuthSpecs, BeSpecs, LeaseSpecs, FeSpecs]}}.

%%====================================================================
%% Internal functions
%%====================================================================
