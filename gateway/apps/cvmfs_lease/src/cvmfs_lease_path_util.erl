%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease_path_util).

-export([are_overlapping/2]).

%%--------------------------------------------------------------------
%% @doc
%% Checks if Path1 and Path2 overlap.
%%
%% Two paths overlap if one is a subpath of the other(foo/bar/baz and
%% foo/bar)
%%
%% @spec are_overlapping(Path1, Path2) -> boolean().
%% @end
%%--------------------------------------------------------------------

are_overlapping(Path1, Path2) when is_binary(Path1), is_binary(Path2),
                                   size(Path1) > 0, size(Path2) > 0 ->
    CommonPrefixLength = binary:longest_common_prefix([Path1, Path2]),
    (CommonPrefixLength =:= size(Path1)) orelse (CommonPrefixLength =:= size(Path2)).

