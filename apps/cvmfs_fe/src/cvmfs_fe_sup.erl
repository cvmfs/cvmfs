%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe top level supervisor.
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupervisorSpecs = #{strategy => one_for_all,
                        intensity => 5,
                        period => 5},
    CvmfsFeRouterSpecs = #{id => cvmfs_fe_router,
                         start => {cvmfs_fe_router, start_link, []},
                         restart => permanent,
                         shutdown => 2000,
                         type => supervisor,
                         modules => [cvmfs_fe_router]},
    {ok, {SupervisorSpecs, [CvmfsFeRouterSpecs]}}.

%%====================================================================
%% Internal functions
%%====================================================================
