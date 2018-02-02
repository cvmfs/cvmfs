%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_test_util).

-export([make_test_user_vars/1,
         make_test_repo_config/0]).

make_test_user_vars(MaxLeaseTime) ->
   #{max_lease_time => MaxLeaseTime,
     fe_tcp_port => 8080,
     receiver_config => [{size, 1},
                         {max_overflow, 0},
                         {worker_module, cvmfs_test_receiver}],
     receiver_worker_config => []
    }.

make_test_repo_config() ->
    #{repos => [{<<"repo1.domain1.org">>, [<<"key1">>]},
                {<<"repo2.domain2.org">>, [<<"key1">>]},
                {<<"repo3.domain3.org">>, [<<"key1">>, <<"key2">>]}],
      keys => [{<<"plain_text">>, <<"key1">>, <<"secret1">>, <<"/">>},
               {<<"plain_text">>, <<"key2">>, <<"secret2">>, <<"/subpath">>}]}.
