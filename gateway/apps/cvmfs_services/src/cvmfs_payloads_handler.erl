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
    Req1 = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/plain-text">>},
                           <<"">>,
                           Req0),
    {ok, Req1, State};
%% @doc
%% A "POST" request to /api/payloads, which can return either 200 OK
%% or in 400 - Bad Request
%%
%% The body of the request should be a JSON payload containing the
%% "user", "session_token" and "payload" fields
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", "error"
%% "reason" - if status is "error", this is a description of the error.
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"POST">>}, State) ->
    {ok, Data, Req1} = cvmfs_fe_util:read_body(Req0),
    {Status, Reply, Req2} = case jsx:decode(Data, [return_maps]) of
                                #{<<"user">> := User,
                                  <<"session_token">> := Token,
                                  <<"payload">> := Payload} ->
                                    Rep = p_submit_payload(User,
                                                           Token,
                                                           Payload),
                                    {200, Rep, Req1};
                                _ ->
                                    {400, #{}, Req1}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),
    {ok, ReqF, State}.


%% Private functions


p_submit_payload(User, Token, Payload) ->
    case cvmfs_be:submit_payload(User, Token, Payload) of
        {ok, payload_added} ->
            #{<<"status">> => <<"ok">>};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.

