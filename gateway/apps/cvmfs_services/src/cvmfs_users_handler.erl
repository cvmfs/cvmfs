%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_users_handler - request handler for the "users" resource
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_users_handler).

-export([init/2]).

%%--------------------------------------------------------------------
%% @doc
%% Handles requests for the /api/users resource, returning the list of
%% registered users
%%
%% @end
%%--------------------------------------------------------------------
init(Req0, State) ->
    Users = cvmfs_auth:get_users(),
    Req = cowboy_req:reply(200,
                           #{<<"content-type">> => <<"application/json">>},
                           jsx:encode(#{<<"users">> => Users}),
                           Req0),
    {ok, Req, State}.

