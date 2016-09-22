%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth_tests).

-include_lib("eunit/include/eunit.hrl").

%% Note: These tests are for the API of the main cvmfs_auth module and
%% since they exercise multiple other modules, they should become
%% integration tests. Should move them to common test (ct), which is
%% better suited for the testing of stateful systems.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
start_stop_test_() ->
    {"The auth server can be started and stopped"
    ,{setup, fun start/0, fun stop/1, fun start_stop/1}}.

get_user_permissions_test_() ->
    [{"A valid username returns appropriate repo entries"
     ,{setup, fun start/0, fun stop/1, fun valid_username_returns_paths/1}}
    ,{"A valid username may have no rights"
     ,{setup, fun start/0, fun stop/1, fun valid_username_can_have_no_paths/1}}
    ,{"An invalid username should return no repo results"
     ,{setup, fun start/0, fun stop/1, fun invalid_username_returns_error/1}}].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    case whereis(cvmfs_auth) of
        undefined ->
            {ok, _} = cvmfs_auth:start_link({cvmfs_auth_test_helper:make_repos(),
                                             cvmfs_auth_test_helper:make_acl()});
        _ ->
            true
    end.

stop(_) ->
    case whereis(cvmfs_auth) of
        undefined ->
            true;
        _ ->
            gen_server:stop(cvmfs_auth)
    end.

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%
start_stop(_) ->
    [?_assert(is_pid(whereis(cvmfs_auth))
              and is_list(ets:info(repos))
              and is_list(ets:info(acl)))].

valid_username_returns_paths(_) ->
    {ok, Results} = cvmfs_auth:get_user_permissions(<<"user1">>),
    [?_assertEqual(Results, [<<"/path/to/repo/1">>
                            ,<<"/path/to/another/repo">>
                            ,<<"/path/to/last/repo">>])].

valid_username_can_have_no_paths(_) ->
    {ok, Results} = cvmfs_auth:get_user_permissions(<<"user4">>),
    [?_assertEqual(Results, [])].

invalid_username_returns_error(_) ->
    Error = cvmfs_auth:get_user_permissions(<<"not_a_username">>),
    [?_assertEqual(Error, user_not_found)].
