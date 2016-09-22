-module(cvmfs_auth_test_helper).

-export([make_repos/0, make_acl/0]).

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

make_repos() ->
    [{<<"repo1">>, <<"/path/to/repo/1">>}
    ,{<<"repo2">>, <<"/path/to/another/repo">>}
    ,{<<"repo3">>, <<"/path/to/last/repo">>}].

make_acl() ->
    [{<<"user1">>, [<<"repo1">>, <<"repo2">>, <<"repo3">>]}
    ,{<<"user2">>, [<<"repo2">>, <<"repo3">>]}
    ,{<<"user3">>, [<<"repo3">>]}
    ,{<<"user4">>, []}].
