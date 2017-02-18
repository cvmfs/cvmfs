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
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    Req1 = cowboy_req:reply(405,
                           #{<<"content-type">> => <<"application/plain-text">>},
                           <<"">>,
                           Req0),

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, Req1, State};
%%--------------------------------------------------------------------
%% @doc
%% A "POST" request to /api/leases, which can return either 200 OK
%% or in 400 - Bad Request
%%
%% The "user" and "path" fields have to be specified in the query string
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", "path_busy" or "error"
%% "session_token" - if status is "ok", this is the session token that
%%                   should be used for all subsequent requests
%% "time_remaining" - if status is "path_busy", this represents the
%%                    time remaining on the current active lease
%% "reason" - if status is "error", this is a description of the error.
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"POST">>}, State) ->
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    {Status, Reply, Req1} = case cowboy_req:match_qs([user, path], Req0) of
                                #{user := User, path := Path} ->
                                    Rep = p_new_lease(User, Path),
                                    {200, Rep, Req0};
                                _ ->
                                    {400, #{}, Req0}
                            end,
    ReqF = cowboy_req:reply(Status,
                            #{<<"content-type">> => <<"application/json">>},
                            jsx:encode(Reply),
                            Req1),

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, ReqF, State};
%%--------------------------------------------------------------------
%% @doc
%% A "DELETE" request to /api/leases/<TOKEN>, which returns 200 OK,
%%
%% The body of the reply, for a valid request contains the fields:
%% "status" - either "ok", or "error"
%% "reason" - if status is "error", this is a description of the error.
%%
%% Making a "DELETE" request to /api/leases (omitting the TOKEN), returns
%% 400 - Bad Request
%% @end
%%--------------------------------------------------------------------
init(Req0 = #{method := <<"DELETE">>}, State) ->
    {URI, T0} = cvmfs_fe_util:tick(Req0, micro_seconds),

    {ok, ReqF, State} = case cowboy_req:binding(id, Req0) of
                            undefined ->
                                Reply = #{<<"status">> => <<"error">>,
                                          <<"reason">> => <<"Missing token. Call /api/v1/leases/<TOKEN>">>},
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
                        end,

    cvmfs_fe_util:tock(URI, T0, micro_seconds),
    {ok, ReqF, State}.


%% Private functions


p_new_lease(User, Path) ->
    case cvmfs_be:new_lease(User, Path) of
        {ok, Token} ->
            #{<<"status">> => <<"ok">>, <<"session_token">> => Token};
        {path_busy, Time} ->
            #{<<"status">> => <<"path_busy">>, <<"time_remaining">> => Time};
        {error, Reason} ->
            #{<<"status">> => <<"error">>, <<"reason">> => atom_to_binary(Reason, latin1)}
    end.
