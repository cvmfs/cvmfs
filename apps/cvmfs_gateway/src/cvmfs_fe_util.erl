%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_util).

-compile([{parse_transform, lager_transform}]).

-export([read_body/1, tick/4, tock/5]).


read_body(Req0) ->
    read_body_rec(Req0, <<"">>).


read_body_rec(Req0, Acc) ->
    case cowboy_req:read_body(Req0,#{length => 1000000000, period => 36000000}) of
        {ok, Data, Req1} ->
            DataSize = size(Data),
            {ok, <<Data:DataSize/binary,Acc/binary>>, Req1};
        {more, Data, Req1} ->
            DataSize = size(Data),
            read_body_rec(Req1, <<Data:DataSize/binary,Acc/binary>>)
    end.


tick(Uid, Method, Req, Unit) ->
    T = erlang:monotonic_time(Unit),
    URI = cowboy_req:uri(Req),
    lager:debug("HTTP request received; Uid: ~p; Method: ~p; URI: ~p", [Uid, Method, URI]),
    {URI, T}.


tock(Uid, Method, URI, T0, Unit) ->
    T1 = erlang:monotonic_time(Unit),
    lager:debug("HTTP request handled; Uid: ~p; Method: ~p; URI: ~p; Time to process = ~p usec",
                [Uid, Method, URI, T1 - T0]).

