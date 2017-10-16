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

-export([start_link/0, commit/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).


start_link() ->
    gen_server:start_link(?MODULE, [], []).


commit(Pid, LeasePath, OldRootHash, NewRootHash) ->
    gen_server:call(Pid, {commit, {LeasePath, OldRootHash, NewRootHash}}).


init([]) ->
    {ok, []}.


handle_call({commit, {LeasePath, OldRootHash, NewRootHash}}, _From, State) ->
    Reply = cvmfs_receiver:commit(LeasePath, OldRootHash, NewRootHash),
    {reply, Reply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(Info, State) ->
    lager:warning("Unknown message received: ~p", [Info]),
    {noreply, State}.
