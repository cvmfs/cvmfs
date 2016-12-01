%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_util).

-export([read_body/1]).

read_body(Req0) ->
    read_body_rec(Req0, <<"">>).

read_body_rec(Req0, Acc) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req1} ->
            DataSize = size(Data),
            {ok, <<Data:DataSize/binary,Acc/binary>>, Req1};
        {more, Data, Req1} ->
            DataSize = size(Data),
            read_body_rec(Req1, <<Data:DataSize/binary,Acc/binary>>)
    end.

