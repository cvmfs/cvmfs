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

non_empty_binary() ->
    non_empty(binary()).

%% For any pair of paths of the form P1 and P2P1 (e.g. foo and foo/bar) P1 is a subpath of P2P1
prop_is_subpath_p1_p2p1() ->
    io:format("Describe test case"),
    ?FORALL({Path1, Path2}
           ,{non_empty_binary(), non_empty_binary()}
           ,cvmfs_lease_path_util:is_subpath(<<Path1/binary,Path2/binary>>,Path1)).
