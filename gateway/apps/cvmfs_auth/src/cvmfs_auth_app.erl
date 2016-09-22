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
    {ok, RepoList} = application:get_env(cvmfs_auth, repo_list),
    {ok, ACL} = application:get_env(cvmfs_auth, acl),

    cvmfs_auth_sup:start_link({RepoList, ACL}).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
