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
         check_repos/1,
         check_leases/1,
         create_and_delete_session/1,
         create_invalid_leases/1,
         create_session_when_already_created/1,
         end_invalid_session/1,
         normal_payload_submission/1]).

-define(API_ROOT, "/api/v1").

%% Test description

all() ->
    [{group, resource_check},
     {group, leases},
     {group, payloads}].

groups() ->
    [
     {resource_check, [], [check_root, check_repos, check_leases]},
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
    ok = ct:require(keys),
    ok = application:set_env(cvmfs_services, enabled_services, [cvmfs_auth, cvmfs_lease, cvmfs_be, cvmfs_fe]),
    ok = application:set_env(cvmfs_services, repo_config, #{repos => ct:get_config(repos)
                                                           ,keys => ct:get_config(keys)}),
    MaxLeaseTime = 200, % milliseconds
    ok = application:set_env(cvmfs_services, max_lease_time, MaxLeaseTime),

    {ok, _} = application:ensure_all_started(cvmfs_services),

    [{max_lease_time, MaxLeaseTime}, {keys, ct:get_config(keys)}] ++ Config.

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
    {ok, Body} = p_get(conn_pid(Config), ?API_ROOT),
    #{<<"resources">> := Resources} = jsx:decode(Body, [return_maps]),
    Resources =:= [<<"users">>, <<"repos">>, <<"leases">>, <<"payloads">>].


check_repos(Config) ->
    {ok, Body} = p_get(conn_pid(Config), ?API_ROOT ++ "/repos"),
    #{<<"repos">> := Repos} = jsx:decode(Body, [return_maps]),
    Repos =:= cvmfs_auth:get_repos().


check_leases(Config) ->
    {error, reply_without_body} = p_get(conn_pid(Config), ?API_ROOT ++ "/leases").


create_and_delete_session(Config) ->
    RequestBody = jsx:encode(#{<<"path">> => <<"repo1.domain1.org">>}),
    HMAC = p_make_hmac(RequestBody, Config),
    RequestHeaders = p_make_headers(RequestBody, <<"key1">>, HMAC),
    {ok, ReplyBody1} = p_post(conn_pid(Config), ?API_ROOT ++ "/leases", RequestHeaders, RequestBody),
    #{<<"session_token">> := Token} = jsx:decode(ReplyBody1, [return_maps]),

    HMAC2 = p_make_hmac(<<"key1">>, Config),
    RequestHeaders2 = p_make_headers(<<"key1">>, HMAC2),
    {ok, ReplyBody2} = p_delete(conn_pid(Config), ?API_ROOT ++ "/leases/" ++ binary_to_list(Token),
                                RequestHeaders2),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody2, [return_maps]).


create_invalid_leases(Config) ->
    RequestReplies = [
                      {<<"bad_key">>, <<"repo1.domain1.org">>, <<"invalid_hmac">>},
                      {<<"key1">>, <<"bad_path">>, <<"invalid_path">>}
                     ],
    Check = fun({KeyId, Path, Reason}) ->
                    RequestBody = jsx:encode(#{<<"path">> => Path}),
                    HMAC = p_make_hmac(RequestBody, Config),
                    RequestHeaders = p_make_headers(RequestBody, KeyId, HMAC),
                    {ok, ReplyBody} = p_post(conn_pid(Config), ?API_ROOT ++ "/leases", RequestHeaders, RequestBody),
                    #{<<"status">> := <<"error">>,
                      <<"reason">> := Reason} = jsx:decode(ReplyBody, [return_maps])
            end,
    lists:foreach(Check, RequestReplies).


create_session_when_already_created(Config) ->
    % Create new lease
    RequestBody = jsx:encode(#{<<"path">> => <<"repo1.domain1.org">>}),
    HMAC = p_make_hmac(RequestBody, Config),
    RequestHeaders = p_make_headers(RequestBody, <<"key1">>, HMAC),
    {ok, ReplyBody1} = p_post(conn_pid(Config), ?API_ROOT ++ "/leases", RequestHeaders, RequestBody),
    #{<<"session_token">> := Token} = jsx:decode(ReplyBody1, [return_maps]),

    % Try to acquire a lease for the same path a second time
    {ok, ReplyBody2} = p_post(conn_pid(Config), ?API_ROOT ++ "/leases", RequestHeaders, RequestBody),
    #{<<"status">> := <<"path_busy">>} = jsx:decode(ReplyBody2, [return_maps]),

    % End lease
    HMAC3 = p_make_hmac(<<"key1">>, Config),
    RequestHeaders3 = p_make_headers(<<"key1">>, HMAC3),
    {ok, ReplyBody3} = p_delete(conn_pid(Config), ?API_ROOT ++ "/leases/" ++ binary_to_list(Token),
                               RequestHeaders3),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody3, [return_maps]).


end_invalid_session(Config) ->
    HMAC = p_make_hmac(<<"key1">>, Config),
    RequestHeaders = p_make_headers(<<"key1">>, HMAC),
    {ok, Body} = p_delete(conn_pid(Config), ?API_ROOT ++ "/leases/NOT_A_PROPER_SESSION_TOKEN", RequestHeaders),
    #{<<"status">> := <<"error">>,
      <<"reason">> := <<"invalid_token">>} = jsx:decode(Body, [return_maps]).


normal_payload_submission(Config) ->
    % Create new lease
    RequestBody1 = jsx:encode(#{<<"path">> => <<"repo1.domain1.org">>}),
    HMAC1 = p_make_hmac(RequestBody1, Config),
    RequestHeaders1 = p_make_headers(RequestBody1, <<"key1">>, HMAC1),
    {ok, ReplyBody1} = p_post(conn_pid(Config), ?API_ROOT ++ "/leases", RequestHeaders1, RequestBody1),
    #{<<"session_token">> := Token} = jsx:decode(ReplyBody1, [return_maps]),

    % Submit payload
    Payload = <<"IAMAPAYLOAD">>,
    JSONMessage = jsx:encode(#{<<"session_token">> => Token}),
    RequestBody2 = <<JSONMessage/binary,Payload/binary>>,
    MessageSize = size(JSONMessage),
    MessageHMAC = p_make_hmac(JSONMessage, Config),
    RequestHeaders2 = p_make_headers(RequestBody2, <<"key1">>, MessageHMAC, MessageSize),
    {ok, ReplyBody2} = p_post(conn_pid(Config), ?API_ROOT ++ "/payloads", RequestHeaders2, RequestBody2),
    #{<<"status">> := <<"ok">>} = jsx:decode(ReplyBody2, [return_maps]),

    % End lease
    HMAC3 = p_make_hmac(<<"key1">>, Config),
    RequestHeaders3 = p_make_headers(<<"key1">>, HMAC3),
    {ok, ReplyBody3} = p_delete(conn_pid(Config), ?API_ROOT ++ "/leases/" ++ binary_to_list(Token),
                               RequestHeaders3),
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


p_delete(ConnPid, Path, Headers) ->
    p_wait(ConnPid, gun:delete(ConnPid, Path, Headers)).


p_make_hmac(Body, Config) ->
    {_, Secret} = lists:keyfind(<<"key1">>, 1, ?config(keys, Config)),
    base64:encode(crypto:hmac(sha, Secret, Body)).


p_make_headers(KeyId, HMAC) ->
    [{<<"authorization">>, <<KeyId/binary, <<" ">>/binary, HMAC/binary>>}].

p_make_headers(Body, KeyId, HMAC) ->
    [{<<"content-type">>, <<"application/json">>},
     {<<"content-length">>, integer_to_binary(size(Body))},
     {<<"authorization">>, <<KeyId/binary, <<" ">>/binary, HMAC/binary>>}].

p_make_headers(Body, KeyId, HMAC, MessageSize) ->
    [{<<"content-type">>, <<"application/json">>},
     {<<"content-length">>, integer_to_binary(size(Body))},
     {<<"authorization">>, <<KeyId/binary, <<" ">>/binary, HMAC/binary>>},
     {<<"message-size">>, integer_to_binary(MessageSize)}].

