%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease_path_util_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, qc_path_overlap_test/1]).

%% Tests description

all() ->
    [qc_path_overlap_test].

qc_path_overlap_test(_Config) ->
    ok = eunit:test(?MODULE).

%% Test cases

non_empty_binary() ->
    non_empty(binary()).

%% Generate a pair of non_empty binaries of the form {B1, B1B2} or {B1B2, B1}
ne_binary_pair() ->
    ?LET({B1, B2},
         {non_empty_binary(), non_empty_binary()},
         proper_types:oneof([{<<B1/binary,B2/binary>>, B1} ,{B1, <<B1/binary,B2/binary>>}])).

%% Two paths of the form P2 and P2P1 (e.g. foo and foo/bar) overlap
prop_are_overlapping() ->
    ?FORALL({Path1, Path2}
           ,ne_binary_pair()
           ,cvmfs_lease_path_util:are_overlapping(Path1, Path2)).

% Test cases
overlap_test() ->
    ?assertEqual(true, proper:quickcheck(prop_are_overlapping())).
