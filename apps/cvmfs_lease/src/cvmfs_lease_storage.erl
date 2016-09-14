%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease storage module
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_lease_storage).

%% API
-export([init/0]).

%% Records used as table entries
-include("cvmfs_lease_storage.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Initializes the storage module. Creates the 'repos', 'acl' and
%%      'leases' tables. The 'acl' table is populated.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok | acl_init_error.
init() ->
    case application:get_env(cvmfs_lease, acl) of
        {ok, ACL} ->
            ets:new(repos, [private, named_table, set, {keypos, #repo_entry.repo_id}]),
            ets:new(acl, [private, named_table, set, {keypos, #acl_entry.client_id}]),
            ets:new(leases, [private, named_table, set, {keypos, #lease_entry.repo_path}]),
            populate_acl(ACL),
            cvmfs_om_log:info("Storage module initialized."),
            ok;
        _ ->
            cvmfs_om_log:error("Error initializing storage module - 'acl' app environment variable not found"),
            acl_init_error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Has side-effects. Will fill the ETS table 'acl'
-spec populate_acl([{binary(), [binary()]}]) -> [true].
populate_acl(ACL) ->
    [ets:insert(acl, #acl_entry{client_id = ClientId,
                                repo_ids = RepoList}) || {ClientId, RepoList} <- ACL].
