%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_auth storage module
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_auth_db).

%% API
-export([init/1, terminate/0, get_user_paths/1]).

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
-spec init({[{binary(), binary()}], [{binary(), [binary()]}]}) -> ok.
init({RepoList, ACL}) ->
    ets:new(repos, [private, named_table, set, {keypos, #repo_entry.repo_id}]),
    ets:new(acl, [private, named_table, set, {keypos, #acl_entry.client_id}]),

    populate_repos(RepoList),
    populate_acl(ACL),

    cvmfs_om_log:info("CVMFS Auth storage module initialized."),
    ok.

terminate() ->
    ets:delete(acl),
    ets:delete(repos),
    ok.

%%--------------------------------------------------------------------
%% @doc Queries the 'acl' and 'repos' tables for the paths which
%%      `User` is allowed to modify
%%
%% @end
%% --------------------------------------------------------------------
-spec get_user_paths(binary()) -> {ok, [binary()]} | user_not_found.
get_user_paths(User) when is_binary(User) ->
    case ets:lookup(acl, User) of
        [] ->
            user_not_found;
        AclEntries ->
                    {ok, [Path || #acl_entry{repo_ids = Repos} <- AclEntries,
                                  Repo <- Repos,
                                  #repo_entry{repo_path = Path} <- ets:lookup(repos, Repo)]}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% has side-effects. Will fill the ETS table 'acl'
-spec populate_acl([{binary(), [binary()]}]) -> [true].
populate_acl(ACL) ->
    [ets:insert(acl, #acl_entry{client_id = ClientId,
                                repo_ids = RepoList}) || {ClientId, RepoList} <- ACL].

%% has side-effects. Will fill the ETS table 'repos'
-spec populate_repos([{binary(), binary()}]) -> [true].
populate_repos(RepoList) ->
    [ets:insert(repos, #repo_entry{repo_id = RepoId,
                                   repo_path = RepoPath}) || {RepoId, RepoPath} <- RepoList].
