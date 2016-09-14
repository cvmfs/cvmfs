%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease storage module header
%%%
%%% @end
%%%-------------------------------------------------------------------

-record(repo_entry, {repo_id :: binary(), repo_path :: binary()}).
-record(acl_entry, {client_id :: binary(), repo_ids :: [binary()]}).
-record(lease_entry, {repo_path :: binary(),
                      client_id :: binary(),
                      session_id :: binary(),
                      timestamp :: integer()}).
