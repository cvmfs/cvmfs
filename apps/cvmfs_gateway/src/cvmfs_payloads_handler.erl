%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_payloads_handler - request handler for the "payloads"
%%%                               resource
%%%
%%% @end
%%%-------------------------------------------------------------------

-compile([{parse_transform, lager_transform}]).

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
    Uid = cvmfs_be:unique_id(),
    {URI, T0} = cvmfs_fe_util:tick(Uid, <<"GET">>, Req0, micro_seconds),

    Req1 = cowboy_req:reply(405,
                            #{<<"content-type">> => <<"application/plain-text">>},
                            <<"">>,
                            Req0),

    cvmfs_fe_util:tock(Uid, <<"GET">>, URI, T0, micro_seconds),
    {ok, Req1, State};
%% @doc
%% A "POST" request to /api/payloads, which can return either 200 OK
%% or in 400 - Bad Request
%%
%% The body of the request should be a JSON message containing the
%% "session_token" field. Directly after the JSON message, representing the
%% remainder of the request body, there is the payload (serialized ObjectPack
%% produced by the CVMFS server tools)
%% The request needs two specific fields in the header:
%%   "authorization" - KeyId and HMAC of the request body (the JSON object)
%%                     The KeyId and HMAC should be separated by a space
%%   "message-size" - the size of the JSON object
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", "error"
%% "reason" - if status is "error", this is a description of the error.
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"POST">>}, State) ->
    Uid = cvmfs_be:unique_id(),
    {URI, T0} = cvmfs_fe_util:tick(Uid, <<"POST">>, Req0, micro_seconds),

    #{headers := #{<<"authorization">> := Auth, <<"message-size">> := MessageSizeBin}} = Req0,
    [KeyId, ClientHMAC] = binary:split(Auth, <<" ">>),
    MessageSize = binary_to_integer(MessageSizeBin),
    {ok, Data, Req1} = cvmfs_fe_util:read_body(Req0),
    <<JSONMessage:MessageSize/binary,Payload/binary>> = Data,
    {Status, Reply, Req2} = case jsx:decode(JSONMessage, [return_maps]) of
                                #{<<"session_token">> := Token
                                 ,<<"payload_digest">> := PayloadDigest
                                 ,<<"header_size">> := HeaderSize
                                 ,<<"api_version">> := ReqProtoVer} ->
                                    Rep = case cvmfs_be:check_hmac(Uid, JSONMessage, KeyId, ClientHMAC) of
                                              true ->
                                                  p_submit_payload(Uid,
                                                                   {Token,
                                                                    Payload,
                                                                    PayloadDigest,
                                                                    binary_to_integer(HeaderSize)},
                                                                   ReqProtoVer);
                                              false ->
                                                  #{<<"status">> => <<"error">>,
                                                    <<"reason">> => <<"invalid_hmac">>}
                                    end,
                                    {200, Rep, Req1};
                                _ ->
                                    lager:error("Could not decode JSON message: ~p", [JSONMessage]),
                                    {400, #{}, Req0}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),

    cvmfs_fe_util:tock(Uid, <<"POST">>, URI, T0, micro_seconds),
    {ok, ReqF, State}.


%% Private functions


p_submit_payload(Uid, SubmissionData, _ReqProtoVer) ->
    case cvmfs_be:submit_payload(Uid, SubmissionData) of
        {ok, payload_added} ->
            #{<<"status">> => <<"ok">>};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.

