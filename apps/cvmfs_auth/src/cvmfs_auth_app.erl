%%%-------------------------------------------------------------------
%% @doc cvmfs_auth public API
%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_auth_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    {ok, RepoConfigFile} = application:get_env(cvmfs_auth, repo_config),
    {ok, MnesiaSchema} = application:get_env(cvmfs_services, mnesia_schema),
    {ok, VarList} = file:consult(RepoConfigFile),
    Vars = maps:from_list(VarList),

    cvmfs_auth_sup:start_link({maps:get(repos, Vars)
                              ,maps:get(acl, Vars)
                              ,MnesiaSchema}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
