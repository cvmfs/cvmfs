%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_payloads_handler - request handler for the "payloads"
%%%                               resource
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_payloads_handler).

-export([init/2]).

%%--------------------------------------------------------------------
%% @doc
%% Handles requests for the /api/payloads resource
%%
%% @end
%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% @doc
%% A "GET" request to /api/payloads returns 405 - method not allowed
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"GET">>}, State) ->
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    Req1 = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/plain-text">>},
                           <<"">>,
                           Req0),

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, Req1, State};
%% @doc
%% A "POST" request to /api/payloads, which can return either 200 OK
%% or in 400 - Bad Request
%%
%% The use and session_token parameters should be passed in the query string,
%% while the payload should be passed in the request body.
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", "error"
%% "reason" - if status is "error", this is a description of the error.
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"POST">>}, State) ->
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    {Status, Reply, Req2} = case cowboy_req:match_qs([user, session_token], Req0) of
                                #{user := User, session_token := Token} ->
                                    {ok, Payload, Req1} = cvmfs_fe_util:read_body(Req0),
                                    Rep = p_submit_payload(User, Token, Payload),
                                    {200, Rep, Req1};
                                _ ->
                                    {400, #{}, Req0}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, ReqF, State}.


%% Private functions


p_submit_payload(User, Token, Payload) ->
    case cvmfs_be:submit_payload(User, Token, Payload) of
        {ok, payload_added} ->
            #{<<"status">> => <<"ok">>};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.

