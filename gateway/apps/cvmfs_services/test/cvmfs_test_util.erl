%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_test_util).

-export([make_test_user_vars/1]).

make_test_user_vars(MaxLeaseTime) ->
   #{max_lease_time => MaxLeaseTime,
     fe_tcp_port => 8080,
     receiver_config => [{size, 1},
                         {max_overflow, 0},
                         {worker_module, cvmfs_test_receiver}],
     receiver_worker_config => []
    }.
