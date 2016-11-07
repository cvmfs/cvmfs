%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_proc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([api_qc/1]).

-export([valid_user_valid_path/1
        ,invalid_user_valid_path/1
        ,valid_user_invalid_path/1
        ,invalid_user_invalid_path/1]).

-export([lease_success/1
        ,submission_with_invalid_token_fails/1
        ,submission_with_expired_token_fails/1
        ,submission_with_different_user_name/1]).


%% Tests description

all() ->
    [{group, specifications}
    ,{group, properties}].

groups() ->
    [{specifications, [], [{group, new_lease}
                          ,{group, submit_payload}]}
    ,{new_lease, [], [valid_user_valid_path
                       ,invalid_user_valid_path
                       ,valid_user_invalid_path
                       ,invalid_user_invalid_path]}
    ,{submit_payload, [], [lease_success
                          ,submission_with_invalid_token_fails
                          ,submission_with_expired_token_fails
                          ,submission_with_different_user_name]}
    ,{properties, [], [api_qc]}].

%% Set up and tear down
init_per_suite(Config) ->
    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:start(mnesia),

    ok = application:load(cvmfs_auth),
    ok = ct:require(repos),
    ok = ct:require(acl),
    ok = application:set_env(cvmfs_auth, repo_config, #{repos => ct:get_config(repos)
                                                       ,acl => ct:get_config(acl)}),
    {ok, _} = application:ensure_all_started(cvmfs_auth),

    MaxLeaseTime = 50, % milliseconds
    ok = application:load(cvmfs_lease),
    ok = application:set_env(cvmfs_lease, max_lease_time, MaxLeaseTime),
    {ok, _} = application:ensure_all_started(cvmfs_lease),

    ok = application:load(cvmfs_proc),
    {ok, _} = application:ensure_all_started(cvmfs_proc),

    lists:flatten([[{max_lease_time, MaxLeaseTime}]
                  ,Config]).

end_per_suite(_Config) ->
    application:stop(cvmfs_proc),
    application:unload(cvmfs_proc),
    application:stop(cvmfs_lease),
    application:unload(cvmfs_lease),
    application:stop(cvmfs_auth),
    application:unload(cvmfs_auth),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Specs

% New lease
% Valid user and valid path should be accepted
valid_user_valid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {ok, _} = cvmfs_proc:new_lease(VUser, VPath).
% Invalid user and valid path should be rejected
invalid_user_valid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {IUser, _} = invalid_user_and_path(VUser, VPath),
    {error, invalid_user} = cvmfs_proc:new_lease(IUser, VPath).
% Valid user and invalid path should be rejected
valid_user_invalid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {_, IPath} = invalid_user_and_path(VUser, VPath),
    {error, invalid_path} = cvmfs_proc:new_lease(VUser, IPath).
% Invalid user and invalid path should be rejected with {error, invalid_user}
invalid_user_invalid_path(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {IUser, IPath} = invalid_user_and_path(VUser, VPath),
    {error, invalid_user} = cvmfs_proc:new_lease(IUser, IPath).

% Submit payload
% Normal lease check
lease_success(_Config) ->
    % Start with a valid user and path and receive a valid lease token
    {User, Path} = valid_user_and_path(),
    Payload = <<"placeholder_for_a_real_payload">>,
    {ok, Token} = cvmfs_proc:new_lease(User, Path),
    % Followup with a payload submission (final = false)
    {ok, payload_added} = cvmfs_proc:submit_payload(User, Token, Payload, false),
    % Submit final payload
    {ok, payload_added, lease_ended} = cvmfs_proc:submit_payload(User, Token, Payload, true),
    % After the lease has been closed, the token should be rejected
    {error, lease_not_found} = cvmfs_proc:submit_payload(User, Token, Payload, true).

% Attempt to submit a payload without first obtaining a token
submission_with_invalid_token_fails(_Config) ->
    {User, _} = valid_user_and_path(),
    Token = <<"invalid_token">>,
    Payload = <<"placeholder">>,
    {error, invalid_macaroon} = cvmfs_proc:submit_payload(User, Token, Payload, false).

% Start a valid lease, make submission after the token has expired
submission_with_expired_token_fails(Config) ->
    {User, Path} = valid_user_and_path(),
    Payload = <<"placeholder">>,
    {ok, Token} = cvmfs_proc:new_lease(User, Path),
    ct:sleep(?config(max_lease_time, Config)),
    {error, lease_expired} = cvmfs_proc:submit_payload(User, Token, Payload, false).

submission_with_different_user_name(_Config) ->
    {VUser, VPath} = valid_user_and_path(),
    {IUser, _} = invalid_user_and_path(VUser, VPath),
    Payload = <<"placeholder">>,
    {ok, Token} = cvmfs_proc:new_lease(VUser, VPath),
    {error, invalid_user} = cvmfs_proc:submit_payload(IUser, Token, Payload, false).


%% Properties
api_qc(_Config) ->
    ?assert(true).

%% Private functions
valid_user_and_path() ->
    [U | _ ] = lists:filter(fun(U1) ->
                                    {ok, Ps} = cvmfs_auth:get_user_permissions(U1),
                                    length(Ps) > 0
                            end,
                            cvmfs_auth:get_users()),
    {ok, [P | _]} = cvmfs_auth:get_user_permissions(U),
    {U, P}.

%% Produces an invalid {User, Path} pair
invalid_user_and_path(VUser, VPath) ->
    {<<VUser/binary,"_invalid">>, <<VPath/binary,"_invalid">>}.
