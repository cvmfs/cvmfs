-module(cvmfs_auth_test_helper).

-export([make_repos/0, make_acl/0]).

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

read_cvmfs_auth_vars() ->
    {ok, [L | _]} = file:consult(<<"config/sys.config">>),
    #{cvmfs_auth := Vars} = maps:from_list(L),
    maps:from_list(Vars).

make_repos() ->
    maps:get(repo_list, read_cvmfs_auth_vars()).

make_acl() ->
    maps:get(acl, read_cvmfs_auth_vars()).
