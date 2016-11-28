%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_fe_lease_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_fe_leases_handler).

-export([init/2]).

init(Req0 = #{method := <<"GET">>}, State) ->
    Req = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/json">>},
                           jsx:encode(#{}),
                           Req0),
    {ok, Req, State};
init(Req0 = #{method := <<"PUT">>}, State) ->
    {Status, Reply, Req1} = decode_request(Req0),
    Req2 = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req1),
    {ok, Req2, State}.


decode_request(Req) ->
    case cowboy_req:has_body(Req) of
        true ->
            {ok, Data, Req1} = cowboy_req:read_body(Req),
            case jsx:decode(Data, [return_maps]) of
                #{<<"user">> := User, <<"path">> := Path} ->
                    Reply = new_lease(User, Path),
                    {200, Reply, Req1};
                _ ->
                    {400, #{}, Req}
            end;
        false ->
            {400, #{}, Req}
    end.

new_lease(User, Path) ->
    case cvmfs_be:new_lease(User, Path) of
        {ok, Token} ->
            #{<<"status">> => <<"ok">>, <<"session_token">> => Token};
        {path_busy, Time} ->
            #{<<"status">> => <<"path_busy">>, <<"time_remaining">> => Time};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.
