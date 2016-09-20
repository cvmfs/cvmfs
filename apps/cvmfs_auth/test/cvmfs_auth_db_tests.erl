-module(cvmfs_auth_db_tests).

-include_lib("eunit/include/eunit.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
start_stop_test_() ->
    {"The ETS tables can be created and destroyed",
     {setup, fun start/0, fun stop/1, fun tables_exist/1}}.


%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    cvmfs_auth_db:init(make_acl()).

stop(_) ->
    cvmfs_auth_db:terminate().

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%
tables_exist(_) ->
    [?_assert(is_list(ets:info(repos)) and is_list(ets:info(acl)))].


%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%
make_acl() ->
    [{<<"user1">>, [<<"repo1">>, <<"repo2">>]}
    ,{<<"user2">>, [<<"repo2">>, <<"repo3">>]}
    ,{<<"user3">>, [<<"repo3">>, <<"repo1">>]}].
