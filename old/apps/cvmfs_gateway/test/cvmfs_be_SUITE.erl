%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_be_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([api_qc/1]).

-export([valid_key_valid_path/1
        ,valid_key_busy_path/1
        ,invalid_key_valid_path/1
        ,valid_key_invalid_path/1]).

-export([end_valid_lease/1
        ,commit_valid_lease/1
        ,cancel_invalid_lease/1
        ,commit_invalid_lease/1
        ,cancel_lease_invalid_macaroon/1
        ,commit_lease_invalid_macaroon/1]).


-export([lease_success/1
        ,submission_with_invalid_token_fails/1
        ,submission_with_expired_token_fails/1]).

-export([multiple_commits/1]).


-define(TEST_UID, <<"TEST_UID">>).


%% utility functions
fake_bin() ->
    <<"fake/binary">>.

fake_hashes() ->
    {fake_bin(), fake_bin()}.

fake_tag() ->
    {fake_bin(), fake_bin(), fake_bin()}.


%% Tests description

all() ->
    [{group, specifications}
    ,{group, properties}].

groups() ->
    [{specifications, [], [{group, new_lease}
                          ,{group, end_lease}
                          ,{group, submit_payload}
                          ,{group, commits}]}
    ,{new_lease, [], [valid_key_valid_path
                     ,valid_key_busy_path
                     ,invalid_key_valid_path
                     ,valid_key_invalid_path]}
    ,{end_lease, [], [end_valid_lease
                     ,commit_valid_lease
                     ,cancel_invalid_lease
                     ,commit_invalid_lease
                     ,cancel_lease_invalid_macaroon
                     ,commit_lease_invalid_macaroon]}
    ,{submit_payload, [], [lease_success
                          ,submission_with_invalid_token_fails
                          ,submission_with_expired_token_fails]}
    ,{commits, [], [multiple_commits]}
    ,{properties, [], [api_qc]}].

%% Set up and tear down
init_per_suite(Config) ->

    application:load(mnesia),
    application:set_env(mnesia, schema_location, ram),
    application:ensure_all_started(mnesia),

    ok = application:load(cvmfs_gateway),
    ok = application:set_env(cvmfs_gateway, enabled_services, [cvmfs_auth,
                                                                cvmfs_lease,
                                                                cvmfs_be,
                                                                cvmfs_receiver_pool,
                                                                cvmfs_fast_receiver_pool,
                                                                cvmfs_commit_sup]),
    ok = application:set_env(cvmfs_gateway, repo_config,
                             cvmfs_test_util:make_test_repo_config()),


    MaxLeaseTime = 1, % seconds
    TestUserVars = cvmfs_test_util:make_test_user_vars(MaxLeaseTime),
    ok = application:set_env(cvmfs_gateway, user_config, TestUserVars),

    {ok, _} = application:ensure_all_started(cvmfs_gateway),

    lists:flatten([[{max_lease_time, MaxLeaseTime}], Config]).

end_per_suite(_Config) ->
    application:stop(cvmfs_gateway),
    application:unload(cvmfs_gateway),
    application:stop(mnesia),
    application:unload(mnesia),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% Specs

% New lease
% Valid key and valid path should be accepted
valid_key_valid_path(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:cancel_lease(?TEST_UID, Token).
% Valid key and busy path should be rejected with remaining time
valid_key_busy_path(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    {path_busy, _} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:cancel_lease(?TEST_UID, Token).
% Invalid key and valid path should be rejected
invalid_key_valid_path(_Config) ->
    {error, invalid_key} = cvmfs_be:new_lease(?TEST_UID, <<"key2">>, <<"repo1.domain1.org">>).
% Valid key and invalid path should be rejected
valid_key_invalid_path(_Config) ->
    {error, invalid_repo} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.com">>).


% End lease
% End valid lease
end_valid_lease(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:cancel_lease(?TEST_UID, Token).

% Commit valid lease
commit_valid_lease(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:commit_lease(?TEST_UID, Token, fake_hashes(), fake_tag()).

% End invalid lease
cancel_invalid_lease(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:cancel_lease(?TEST_UID, Token),
    ok = cvmfs_be:cancel_lease(?TEST_UID, Token).

% Commit invalid lease
commit_invalid_lease(_Config) ->
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, <<"key1">>, <<"repo1.domain1.org">>),
    ok = cvmfs_be:commit_lease(?TEST_UID, Token, fake_hashes(), fake_tag()),
    {error, invalid_lease} = cvmfs_be:commit_lease(?TEST_UID,
                                                   Token,
                                                   fake_hashes(),
                                                   fake_tag()).

% End lease invalid macaroon
cancel_lease_invalid_macaroon(_Config) ->
    Token = <<"fake_token">>,
    {error, invalid_macaroon} = cvmfs_be:cancel_lease(?TEST_UID, Token).

% Commit lease invalid macaroon
commit_lease_invalid_macaroon(_Config) ->
    Token = <<"fake_token">>,
    {error, invalid_macaroon} = cvmfs_be:commit_lease(?TEST_UID,
                                                      Token,
                                                      fake_hashes(),
                                                      fake_tag()).


% Submit payload
% Normal lease check
lease_success(_Config) ->
    % Start with a valid key and path and receive a valid lease token
    {Key, Path} = {<<"key1">>, <<"repo1.domain1.org">>},
    Payload = <<"placeholder_for_a_real_payload">>,
    Digest = base64:encode(<<"placeholder_for_the_digest_of_the_payload">>),
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, Key, Path),
    % Followup with a payload submission
    {ok, payload_added} = cvmfs_be:submit_payload(?TEST_UID, {Token, Payload, Digest, 1}),
    % Submit final payload and commit the lease
    {ok, payload_added} = cvmfs_be:submit_payload(?TEST_UID, {Token, Payload, Digest, 1}),
    ok = cvmfs_be:commit_lease(?TEST_UID, Token, fake_hashes(), fake_tag()),
    % After the lease has been closed, the token should be rejected
    {error, invalid_lease} = cvmfs_be:submit_payload(?TEST_UID, {Token, Payload, Digest, 1}).

% Attempt to submit a payload without first obtaining a token
submission_with_invalid_token_fails(_Config) ->
    Token = <<"invalid_token">>,
    Payload = <<"placeholder">>,
    Digest = base64:encode(<<"placeholder_for_the_digest_of_the_payload">>),
    {error, invalid_macaroon} = cvmfs_be:submit_payload(?TEST_UID, {Token, Payload, Digest, 1}).

% Start a valid lease, make submission after the token has expired
submission_with_expired_token_fails(Config) ->
    {Key, Path} = {<<"key1">>, <<"repo1.domain1.org">>},
    Payload = <<"placeholder">>,
    Digest = base64:encode(<<"placeholder_for_the_digest_of_the_payload">>),
    {ok, Token} = cvmfs_be:new_lease(?TEST_UID, Key, Path),
    ct:sleep(?config(max_lease_time, Config) * 1000),
    {error, lease_expired} = cvmfs_be:submit_payload(?TEST_UID, {Token, Payload, Digest, 1}).


% Attempt concurrent commits
multiple_commits(_Config) ->
    Parent = self(),
    spawn_link(fun() ->
                  {Key, Path} = {<<"key1">>, <<"repo1.domain1.org/one">>},
                  {ok, Token} = cvmfs_be:new_lease(?TEST_UID, Key, Path),
                  Parent ! cvmfs_be:commit_lease(?TEST_UID,
                                                 Token,
                                                 {<<"old_hash">>, <<"new_hash">>},
                                                 fake_tag())
          end),
    spawn_link(fun() ->
                  {Key, Path} = {<<"key1">>, <<"repo1.domain1.org/two">>},
                  {ok, Token} = cvmfs_be:new_lease(?TEST_UID, Key, Path),
                  Parent ! cvmfs_be:commit_lease(?TEST_UID,
                                                 Token,
                                                 {<<"old_hash">>, <<"new_hash">>},
                                                 fake_tag())
          end),
    receive
        Res1 ->
            ok = Res1
    end,
    receive
        Res2 ->
            ok = Res2
    end.


%% Properties
api_qc(_Config) ->
    ?assert(true).

