%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(prop_cvmfs_lease_path_util).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

non_empty_binary() ->
    non_empty(binary()).

%% Two paths of the form P1 and P2P1 (e.g. foo and foo/bar) overlap
prop_are_overlapping_p2_p2p1() ->
    ?FORALL({Path1, Path2}
           ,{non_empty_binary(), non_empty_binary()}
           ,cvmfs_lease_path_util:are_overlapping(Path2, <<Path2/binary,Path1/binary>>)).

%% Two paths of the form P2P1 and P2 (e.g. foo/bar and foo) overlap
prop_are_overlapping_p2p1_p2() ->
    ?FORALL({Path1, Path2}
           ,{non_empty_binary(), non_empty_binary()}
           ,cvmfs_lease_path_util:are_overlapping(<<Path2/binary,Path1/binary>>,Path2)).
