%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% This module implements a simple OTP gen_server which serializes
%%% commit operations to a single repository. The gen_server is
%%% stateless and its only task is to forward, one at a time, the
%%% commit requests for a single repository, to the cvmfs_receiver
%%% worker.
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_commit_worker).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

-export([start_link/1, commit/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).


start_link(RepoName) ->
    gen_server:start_link(?MODULE, RepoName, []).


-spec commit(Pid, LeasePath, OldRootHash, NewRootHash, RepoTag) ->
                     ok | Error
                        when Pid :: pid(),
                             LeasePath :: binary(),
                             OldRootHash :: binary(),
                             NewRootHash :: binary(),
                             RepoTag :: cvmfs_common_types:repository_tag(),
                             Error :: cvmfs_common_types:commit_error().
commit(Pid, LeasePath, OldRootHash, NewRootHash, RepoTag) ->
    gen_server:call(Pid, {commit, {LeasePath, OldRootHash, NewRootHash, RepoTag}},
                    cvmfs_app_util:get_max_lease_time()).


init(RepoName) ->
    ets:insert(commit_workers, {RepoName, self()}),
    {ok, []}.


handle_call({commit, {LeasePath, OldRootHash, NewRootHash, RepoTag}}, _From, State) ->
    Reply = cvmfs_receiver:commit(LeasePath, OldRootHash, NewRootHash, RepoTag),
    {reply, Reply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(Info, State) ->
    lager:warning("Unknown message received: ~p", [Info]),
    {noreply, State}.
