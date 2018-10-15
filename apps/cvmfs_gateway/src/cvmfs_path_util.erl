%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_path_util).

-export([are_overlapping/2
        ,split_repo_path/1]).

%%--------------------------------------------------------------------
%% @doc
%% Checks if Path1 and Path2 overlap.
%%
%% Two paths overlap if one is a subpath of the other(foo/bar/baz and
%% foo/bar)
%%
%% @end
%%--------------------------------------------------------------------
-spec are_overlapping(Path1 :: binary(), Path2 :: binary()) -> boolean().
are_overlapping(Path1, Path2) when size(Path1) == 0; size(Path2) == 0 ->
    true;
are_overlapping(Path1, Path2) when size(Path1) > 0, size(Path2) > 0 ->
    SplitPath1 = filename:split(drop_leading_slash(Path1)),
    SplitPath2 = filename:split(drop_leading_slash(Path2)),

    lists:prefix(SplitPath1, SplitPath2) or lists:prefix(SplitPath2, SplitPath1).

drop_leading_slash(<<"/",Rest/binary>>) ->
    Rest;
drop_leading_slash(Path) ->
    Path.


-spec split_repo_path(Path :: binary()) -> {Repo :: binary(), Subpath :: binary()}.
split_repo_path(Path) when is_binary(Path) ->
    case binary:split(Path, <<"/">>) of
        [Repo | []] ->
            {Repo, <<"">>};
        [Repo, Subpath] ->
            {Repo, Subpath}
    end.
