%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease_path_util).

-export([is_subpath/2]).

%%--------------------------------------------------------------------
%% @doc
%% Checks if Path1 is a subpath of Path2
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec is_subpath(binary(), binary()) -> boolean().
is_subpath(Path1, Path2) when is_binary(Path1), is_binary(Path2),
                              size(Path1) > 0, size(Path2) > 0 ->
    binary:longest_common_prefix([Path1, Path2]) =:= size(Path2).

