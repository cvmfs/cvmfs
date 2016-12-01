%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_payloads_handler
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_payloads_handler).

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
                                #{<<"user">> := User,
                                  <<"session_token">> := Token,
                                  <<"payload">> := Payload,
                                  <<"final">> := Final} ->
                                    Rep = p_submit_payload(User,
                                                           Token,
                                                           Payload,
                                                           binary_to_atom(Final, latin1)),
                                    {200, Rep, Req1};
                                _ ->
                                    {400, #{}, Req1}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),
    {ok, ReqF, State}.

p_submit_payload(User, Token, Payload, Final) ->
    case cvmfs_be:submit_payload(User, Token, Payload, Final) of
        {ok, payload_added} ->
            #{<<"status">> => <<"ok">>};
        {ok, payload_added, lease_ended} ->
            #{<<"status">> => <<"ok">>};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.

