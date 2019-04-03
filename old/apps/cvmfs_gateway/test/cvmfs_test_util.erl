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
         make_test_repo_config/0,
         make_test_repo_config_v1/0]).

make_test_user_vars(MaxLeaseTime) ->
    #{max_lease_time => MaxLeaseTime,
      fe_tcp_port => 4929,
      receiver_config =>
          #{size => 1,
            max_overflow => 0,
            worker_module => cvmfs_test_receiver},
      receiver_worker_config => #{}
     }.

make_test_repo_config() ->
    #{version => 2,
      repos => [#{domain => <<"repo1.domain1.org">>,
                  keys => [#{id => <<"key1">>, path => <<"/">>}]},
                #{domain => <<"repo2.domain2.org">>,
                  keys => [#{id => <<"key1">>, path => <<"/">>}]},
                #{domain => <<"repo3.domain3.org">>,
                  keys => [#{id => <<"key1">>, path => <<"/">>},
                           #{id => <<"key2">>, path => <<"/subpath">>}]}],
      keys => [#{type => <<"plain_text">>,
                 id => <<"key1">>,
                 secret => <<"secret1">>},
               #{type => <<"plain_text">>,
                 id => <<"key2">>,
                 secret => <<"secret2">>}]
     }.

make_test_repo_config_v1() ->
    #{repos => [#{domain => <<"repo1.domain1.org">>,
                  keys => [<<"key1">>]},
                #{domain => <<"repo2.domain2.org">>,
                  keys => [<<"key1">>]},
                #{domain => <<"repo3.domain3.org">>,
                  keys => [<<"key1">>, <<"key2">>]}],
      keys => [#{type => <<"plain_text">>,
                 id => <<"key1">>,
                 secret => <<"secret1">>,
                 repo_subpath => <<"/">>},
               #{type => <<"plain_text">>,
                 id => <<"key2">>,
                 secret => <<"secret2">>,
                 repo_subpath => <<"/subpath">>}]
     }.
