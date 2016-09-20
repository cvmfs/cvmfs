%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_auth storage module
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_auth_db).

%% API
-export([init/0, init/1, terminate/0, get_user_credentials/1]).

%% Records used as table entries
-record(repo_entry, {repo_id :: binary(), repo_path :: binary()}).
-record(acl_entry, {client_id :: binary(), repo_ids :: [binary()]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Initializes the storage module. Creates the 'repos' and 'acl'
%% tables. The 'acl' table is populated.
%%
%% @end
%% --------------------------------------------------------------------
-spec init() -> ok | acl_init_error.
init() ->
    case application:get_env(cvmfs_auth, acl) of
        {ok, ACL} ->
            init(ACL);
        _ ->
            cvmfs_om_log:error("Error initializing storage module - 'acl' app environment variable not found"),
            acl_init_error
    end.

init(ACL) ->
    ets:new(repos, [private, named_table, set, {keypos, #repo_entry.repo_id}]),
    ets:new(acl, [private, named_table, set, {keypos, #acl_entry.client_id}]),
    populate_acl(ACL),
    cvmfs_om_log:info("CVMFS Auth storage module initialized."),
    ok.

terminate() ->
    ets:delete(repos),
    ets:delete(acl),
    ok.

%%--------------------------------------------------------------------
%% @doc Queries the 'acl' table for the entry corresponding to 'User'
%% and returns the list of repository paths which the user is allowed
%% to modify
%%
%% @end
%% --------------------------------------------------------------------
-spec get_user_credentials(binary()) -> [binary()].
get_user_credentials(User) when is_binary(User) ->
    [#acl_entry{client_id = User, repo_ids = Repos} | _] = ets:lookup(acl, User),
    RepoPathSelector = fun (Repo, Acc) ->
                               [#repo_entry{repo_id = Repo, repo_path = P} | _] = ets:lookup(repos, Repo),
                               [P | Acc]
                       end,
    lists:foldl(RepoPathSelector, [], Repos).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% has side-effects. Will fill the ETS table 'acl'
-spec populate_acl([{binary(), [binary()]}]) -> [true].
populate_acl(ACL) ->
    [ets:insert(acl, #acl_entry{client_id = ClientId,
                                repo_ids = RepoList}) || {ClientId, RepoList} <- ACL].
