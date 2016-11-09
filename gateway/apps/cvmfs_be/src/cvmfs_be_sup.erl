%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_be_sup public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_be_sup).

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
    CvmfsBeMainSpecs = #{id => cvmfs_be,
                         start => {cvmfs_be, start_link, [{}]},
                         restart => permanent,
                         shutdown => 2000,
                         type => worker,
                         modules => [cvmfs_be]},
    {ok, {SupervisorSpecs, [CvmfsBeMainSpecs]}}.

%%====================================================================
%% Internal functions
%%====================================================================
