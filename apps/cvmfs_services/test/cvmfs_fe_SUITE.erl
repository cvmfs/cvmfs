%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_fe_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([check_root/1,
         check_users/1,
         check_repos/1,
         check_leases/1,
         create_and_delete_session/1,
         create_invalid_leases/1,
         create_session_when_already_created/1,
         end_invalid_session/1,
         normal_payload_submission/1]).


%% Test description

all() ->
    [{group, resource_check},
     {group, leases},
     {group, payloads}].

groups() ->
    [
     {resource_check, [], [check_root, check_users, check_repos, check_leases]},
     {leases, [], [create_and_delete_session,
                   create_invalid_leases,
                   create_session_when_already_created,
                   end_invalid_session]},
     {payloads, [], [normal_payload_submission]}
    ].


%% Set up and tear down

init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    application:ensure_all_started(gun),

    ok = application:load(cvmfs_services),
    ok = ct:require(repos),
    ok = ct:require(acl),
    ok = application:set_env(cvmfs_services, enabled_services, [cvmfs_auth, cvmfs_lease, cvmfs_be, cvmfs_fe]),
    ok = application:set_env(cvmfs_services, repo_config, #{repos => ct:get_config(repos)
                                                           ,acl => ct:get_config(acl)}),
    MaxLeaseTime = 50, % milliseconds
    ok = application:set_env(cvmfs_services, max_lease_time, MaxLeaseTime),

    {ok, _} = application:ensure_all_started(cvmfs_services),

    [{max_lease_time, MaxLeaseTime} | Config].

end_per_suite(_Config) ->
    application:stop(cvmfs_services),
    application:unload(cvmfs_services),
    application:stop(gun),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, ConnPid} = gun:open("localhost", 8080),
    {ok, http} = gun:await_up(ConnPid),
    [{gun_connection, ConnPid} | Config].

end_per_testcase(_TestCase, Config) ->
    ConnPid = ?config(gun_connection, Config),
    ok = gun:shutdown(ConnPid),
    ok.


%% Test specifications

check_root(Config) ->
    {ok, Body} = p_get(conn_pid(Config), "/api"),
    #{<<"resources">> := Resources} = jsx:decode(Body, [return_maps]),
    Resources =:= [<<"users">>, <<"repos">>, <<"leases">>, <<"payloads">>].


check_users(Config) ->
    {ok, Body} = p_get(conn_pid(Config), "/api/users"),
    #{<<"users">> := Users} = jsx:decode(Body, [return_maps]),
    Users =:= cvmfs_auth:get_users().


check_repos(Config) ->
    {ok, Body} = p_get(conn_pid(Config), "/api/repos"),
    #{<<"repos">> := Repos} = jsx:decode(Body, [return_maps]),
    Repos =:= cvmfs_auth:get_repos().


check_leases(Config) ->
    {error, reply_without_body} = p_get(conn_pid(Config), "/api/leases").


create_and_delete_session(Config) ->
    RequestBody = jsx:encode(#{<<"user">> => <<"user1">>, <<"path">> => <<"repo1.domain1.org">>}),
    RequestHeaders = p_make_headers(RequestBody),
    {ok, ReplyBody1} = p_post(conn_pid(Config), "/api/leases", RequestHeaders, RequestBody),
    #{<<"session_token">> := Token} = jsx:decode(ReplyBody1, [return_maps]),
    {ok, ReplyBody2} = p_delete(conn_pid(Config), "/api/leases/" ++ binary_to_list(Token)),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody2, [return_maps]).


create_invalid_leases(Config) ->
    RequestReplies = [
                      {<<"bad_user">>, <<"repo1.domain1.org">>, <<"invalid_user">>},
                      {<<"user1">>, <<"bad_path">>, <<"invalid_path">>}
                     ],
    Check = fun({User, Path, Reason}) ->
                     RequestBody = jsx:encode(#{<<"user">> => User, <<"path">> => Path}),
                     RequestHeaders = p_make_headers(RequestBody),
                     {ok, ReplyBody} = p_post(conn_pid(Config), "/api/leases", RequestHeaders, RequestBody),
                     #{<<"status">> := <<"error">>,
                       <<"reason">> := Reason} = jsx:decode(ReplyBody, [return_maps])
             end,
    lists:foreach(Check, RequestReplies).


create_session_when_already_created(Config) ->
    RequestBody = jsx:encode(#{<<"user">> => <<"user1">>, <<"path">> => <<"repo1.domain1.org">>}),
    RequestHeaders = p_make_headers(RequestBody),
    {ok, ReplyBody1} = p_post(conn_pid(Config), "/api/leases", RequestHeaders, RequestBody),
    #{<<"session_token">> := _Token} = jsx:decode(ReplyBody1, [return_maps]),
    {ok, ReplyBody2} = p_post(conn_pid(Config), "/api/leases", RequestHeaders, RequestBody),
    #{<<"status">> := <<"path_busy">>} = jsx:decode(ReplyBody2, [return_maps]).


end_invalid_session(Config) ->
    {ok, Body} = p_delete(conn_pid(Config), "/api/leases/NOT_A_PROPER_SESSION_TOKEN"),
    #{<<"status">> := <<"error">>,
      <<"reason">> := <<"invalid_token">>} = jsx:decode(Body, [return_maps]).


normal_payload_submission(Config) ->
    % Create new lease
    RequestBody1 = jsx:encode(#{<<"user">> => <<"user1">>, <<"path">> => <<"repo1.domain1.org">>}),
    RequestHeaders1 = p_make_headers(RequestBody1),
    {ok, ReplyBody1} = p_post(conn_pid(Config), "/api/leases", RequestHeaders1, RequestBody1),
    #{<<"session_token">> := Token} = jsx:decode(ReplyBody1, [return_maps]),

    % Submit payload
    Payload = <<"IAMAPAYLOAD">>,
    RequestBody2 = jsx:encode(#{<<"user">> => <<"user1">>, <<"session_token">> => Token,
                                <<"payload">> => Payload}),
    RequestHeaders2 = p_make_headers(RequestBody2),
    {ok, ReplyBody2} = p_post(conn_pid(Config), "/api/payloads", RequestHeaders2, RequestBody2),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody2, [return_maps]),

    % End lease
    {ok, ReplyBody3} = p_delete(conn_pid(Config), "/api/leases/" ++ binary_to_list(Token)),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody3, [return_maps]).


%% Private functions

conn_pid(Config) ->
    ?config(gun_connection, Config).


p_wait(ConnPid, StreamRef) ->
    case gun:await(ConnPid, StreamRef, 1000) of
        {response, nofin, _Status, _Headers} ->
            gun:await_body(ConnPid, StreamRef);
        {response, fin, _Status, _Headers} ->
            {error, reply_without_body}
    end.


-spec p_get(Config, Path) -> {ok, Body} | {error, Reason}
                                  when Config :: [{atom(), term()}],
                                       Path :: string(),
                                       Body :: binary(),
                                       Reason :: term().
p_get(ConnPid, Path) ->
    p_wait(ConnPid, gun:get(ConnPid, Path)).


p_post(ConnPid, Path, Headers, Body) ->
    StreamRef = gun:post(ConnPid, Path, Headers),
    gun:data(ConnPid, StreamRef, fin, Body),
    p_wait(ConnPid, StreamRef).


p_delete(ConnPid, Path) ->
    p_wait(ConnPid, gun:delete(ConnPid, Path)).


p_make_headers(Body) ->
    [{<<"content-type">>, <<"application/json">>},
     {<<"content-length">>, integer_to_binary(size(Body))}].
