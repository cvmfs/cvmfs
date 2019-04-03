%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_leases_handler - request handler for the "leases" resource
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_leases_handler).

-export([init/2]).

%%--------------------------------------------------------------------
%% @doc
%% Handles requests for the /api/leases resource
%%
%% @end
%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% @doc
%% A "GET" request to /api/leases returns 405 - method not allowed
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
%%--------------------------------------------------------------------
%% @doc
%% A "POST" request to /api/leases/[<TOKEN>], which can return either 200 OK
%% or in 400 - Bad Request
%%
%% This function only dispatches the work to another function, based on the
%% binding used for the request.
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"POST">>}, State) ->
    Uid = cvmfs_be:unique_id(),
    {URI, T0} = cvmfs_fe_util:tick(Uid, <<"POST">>, Req0, micro_seconds),

    {ok, ReqF, State} = case cowboy_req:binding(token, Req0) of
        undefined ->
            p_handle_new_lease(Req0, State, Uid);
        _Token ->
            p_handle_commit_lease(Req0, State, Uid)
    end,

    cvmfs_fe_util:tock(Uid, <<"POST">>, URI, T0, micro_seconds),
    {ok, ReqF, State};
%%--------------------------------------------------------------------
%% @doc
%% A "DELETE" request to /api/leases/<TOKEN>, which returns
%% 200 OK.
%%
%% The request needs the "authorization" field in the header, containing
%% the KeyId and the HMAC of the KeyId (computed with the secret key associated
%% with KeyId).
%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", or "error"
%% "reason" - if status is "error", this is a description of the error.
%%
%% Making a "DELETE" request to /api/leases (omitting the TOKEN and
%% the KEY_ID), returns
%% 400 - Bad Request
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"DELETE">>}, State) ->
    Uid = cvmfs_be:unique_id(),
    {URI, T0} = cvmfs_fe_util:tick(Uid, <<"DELETE">>, Req0, micro_seconds),

    #{headers := #{<<"authorization">> := Auth}} = Req0,
    [KeyId, ClientHMAC] = binary:split(Auth, <<" ">>),
    {ok, ReqF, State} = case cowboy_req:binding(token, Req0) of
                            undefined ->
                                Reply = #{<<"status">> => <<"error">>,
                                          <<"reason">> => <<"Missing token. Call /api/v1/leases/<TOKEN>">>},
                                Req1 = cowboy_req:reply(400,
                                                        #{<<"content-type">> => <<"application/json">>},
                                                        jsx:encode(Reply),
                                                        Req0),
                                {ok, Req1, State};
                            Token ->
                                Reply = case p_check_hmac(Uid, Token, KeyId, ClientHMAC) of
                                            true ->
                                                case cvmfs_be:cancel_lease(Uid, Token) of
                                                    ok ->
                                                        #{<<"status">> => <<"ok">>};
                                                    {error, invalid_macaroon} ->
                                                        #{<<"status">> => <<"error">>,
                                                          <<"reason">> => <<"invalid_token">>}
                                                end;
                                            false ->
                                                #{<<"status">> => <<"error">>,
                                                  <<"reason">> => <<"invalid_hmac">>}
                                        end,
                                Req1 = cowboy_req:reply(200,
                                                        #{<<"content-type">> => <<"application/json">>},
                                                        jsx:encode(Reply),
                                                        Req0),
                                {ok, Req1, State}
                        end,

    cvmfs_fe_util:tock(Uid, <<"DELETE">>, URI, T0, micro_seconds),
    {ok, ReqF, State}.


%% Private functions

%%--------------------------------------------------------------------
%% @doc
%% A "POST" request to /api/leases, which can return either 200 OK
%% or in 400 - Bad Request
%%
%% The request body is a JSON object with the "path" field
%% The request needs the "authorization" field in the header:
%%   "authorization" - KeyId and HMAC of the request body (the JSON object)
%%                     The KeyId and HMAC should be separated by a space
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", "path_busy" or "error"
%% "session_token" - if status is "ok", this is the session token that
%%                   should be used for all subsequent requests
%% "time_remaining" - if status is "path_busy", this represents the
%%                    time remaining (in seconds) on the current active lease
%% "reason" - if status is "error", this is a description of the error.
%% @end
%%--------------------------------------------------------------------
p_handle_new_lease(Req0, State, Uid) ->
    #{headers := #{<<"authorization">> := Auth}} = Req0,
    [KeyId, ClientHMAC] = binary:split(Auth, <<" ">>),
    {ok, Data, Req1} = cvmfs_fe_util:read_body(Req0),
    {Status, Reply, Req2} = case jsx:decode(Data, [return_maps]) of
                                #{<<"path">> := Path, <<"api_version">> := ReqProtoVer} ->
                                    Rep = case p_check_hmac(Uid, Data, KeyId, ClientHMAC) of
                                        true ->
                                            p_new_lease(Uid, KeyId, Path, p_to_integer(ReqProtoVer));
                                        false ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"invalid_hmac">>}
                                    end,
                                    {200, Rep, Req1};
                                _ ->
                                    {400, #{}, Req1}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req2),

    {ok, ReqF, State}.


%%--------------------------------------------------------------------
%% @doc
%% A "POST" request to /api/leases/<TOKEN>, which returns
%% 200 OK or 400 - Bad Request
%%
%% This requests represents a "Commit" command for the lease associated
%% with <TOKEN>. If successful, the changes made during this lease are
%% applied and reflected in the new state of the repository.
%%
%% The request needs the "authorization" field in the header, containing
%% the KeyId and the HMAC of the KeyId (computed with the secret key associated
%% with KeyId).
%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", or "error"
%% "reason" - if status is "error", this is a description of the error.
%%
%% @end
%%--------------------------------------------------------------------
p_handle_commit_lease(Req0, State, Uid) ->
    #{headers := #{<<"authorization">> := Auth}} = Req0,
    [KeyId, ClientHMAC] = binary:split(Auth, <<" ">>),
    case cowboy_req:binding(token, Req0) of
        undefined ->
            Reply = #{<<"status">> => <<"error">>,
                      <<"reason">> => <<"Missing token. Call /api/v1/leases/<TOKEN>">>},
            Req1 = cowboy_req:reply(400,
                                    #{<<"content-type">> => <<"application/json">>},
                                    jsx:encode(Reply),
                                    Req0),
            {ok, Req1, State};
        Token ->
            {ok, Data, Req1} = cvmfs_fe_util:read_body(Req0),
            Reply = case jsx:decode(Data, [return_maps]) of
                        #{<<"old_root_hash">> := OldRootHash,
                          <<"new_root_hash">> := NewRootHash,
                          <<"tag_name">> := TagName,
                          <<"tag_channel">> := TagChannel,
                          <<"tag_description">> := TagMessage} ->
                            RepoTag = {TagName, TagChannel, TagMessage},
                            case p_check_hmac(Uid, Token, KeyId, ClientHMAC) of
                                true ->
                                    case cvmfs_be:commit_lease(Uid,
                                                               Token,
                                                               {OldRootHash, NewRootHash},
                                                               RepoTag) of
                                        ok ->
                                            #{<<"status">> => <<"ok">>};
                                        {error, invalid_macaroon} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"invalid_token">>};
                                        {error, merge_error} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"merge_error">>};
                                        {error, io_error} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"io_error">>};
                                        {error, misc_error} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"miscellaneous">>};
                                        {error, missing_reflog} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"missing_reflog">>};
                                        {error, worker_died} ->
                                            #{<<"status">> => <<"error">>,
                                              <<"reason">> => <<"worker_died">>}
                                    end;
                                false ->
                                    #{<<"status">> => <<"error">>,
                                      <<"reason">> => <<"invalid_hmac">>}
                            end;
                        _ ->
                            #{}
                    end,
            ReqF = cowboy_req:reply(200,
                                    #{<<"content-type">> => <<"application/json">>},
                                    jsx:encode(Reply),
                                    Req1),
            {ok, ReqF, State}
    end.


p_check_hmac(Uid, JSONMessage, KeyId, ClientHMAC) ->
    cvmfs_be:check_hmac(Uid, JSONMessage, KeyId, ClientHMAC).


p_new_lease(Uid, KeyId, Path, ReqProtoVer) ->
    case ReqProtoVer >= cvmfs_version:min_api_protocol_version() of
        true ->
            case cvmfs_be:new_lease(Uid, KeyId, Path) of
                {ok, Token} ->
                    #{<<"status">> => <<"ok">>,
                      <<"session_token">> => Token,
                      <<"max_api_version">> => integer_to_binary(erlang:min(cvmfs_version:api_protocol_version(),
                                                                            ReqProtoVer))};
                {path_busy, Time} ->
                    #{<<"status">> => <<"path_busy">>, <<"time_remaining">> => Time div 1000 };
                {error, Reason} ->
                    #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
            end;
        false ->
            Msg = "request has incompatible protocol version: " ++ integer_to_list(ReqProtoVer)
                  ++ ", min version: " ++ integer_to_list(cvmfs_version:min_api_protocol_version()),
            #{<<"status">> => <<"error">>, <<"reason">> => list_to_binary(Msg)}
    end.


p_to_integer(V) when is_binary(V) ->
    binary_to_integer(V);
p_to_integer(V) when is_integer(V) ->
    V.

