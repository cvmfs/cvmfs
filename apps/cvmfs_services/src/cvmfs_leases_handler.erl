%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_leases_handler).

-export([init/2]).

init(Req0 = #{method := <<"GET">>}, State) ->
    Req1 = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/plain-text">>},
                           <<"">>,
                           Req0),
    {ok, Req1, State};
init(Req0 = #{method := <<"POST">>}, State) ->
    {ok, Data, Req1} = cvmfs_fe_util:read_body(Req0),
    {Status, Reply, Req2} = case jsx:decode(Data, [return_maps]) of
                                #{<<"user">> := User, <<"path">> := Path} ->
                                    Rep = p_new_lease(User, Path),
                                    {200, Rep, Req1};
                                _ ->
                                    {400, #{}, Req1}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),
    {ok, ReqF, State};
init(Req0 = #{method := <<"DELETE">>}, State) ->
    case cowboy_req:binding(id, Req0) of
        undefined ->
            Reply = #{<<"status">> => <<"error">>,
                      <<"reason">> => <<"BAD Request. Missing token. Call /api/leases/<TOKEN>">>},
            Req1 = cowboy_req:reply(400,
                                    #{<<"content-type">> => <<"application/json">>},
                                    jsx:encode(Reply),
                                    Req0),
            {ok, Req1, State};
        Token ->
            Reply = case cvmfs_be:end_lease(Token) of
                        ok ->
                            #{<<"status">> => <<"ok">>};
                        {error, invalid_macaroon} ->
                            #{<<"status">> => <<"error">>,
                              <<"reason">> => <<"invalid_token">>}
                    end,
            Req1 = cowboy_req:reply(200,
                                    #{<<"content-type">> => <<"application/json">>},
                                    jsx:encode(Reply),
                                    Req0),
            {ok, Req1, State}
    end.


p_new_lease(User, Path) ->
    case cvmfs_be:new_lease(User, Path) of
        {ok, Token} ->
            #{<<"status">> => <<"ok">>, <<"session_token">> => Token};
        {path_busy, Time} ->
            #{<<"status">> => <<"path_busy">>, <<"time_remaining">> => Time};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.
