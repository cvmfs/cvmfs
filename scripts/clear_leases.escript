#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_services_ops -setcookie cvmfs

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main([]) ->
    io:format("Node name: ~p.~n", [node()]),
    io:format("Clearing all active leases... "),
    MainNodeAtom = list_to_atom("cvmfs_services@" ++ net_adm:localhost()),
    ok = rpc:call(MainNodeAtom, cvmfs_lease, clear_leases, []),
    io:format("done.~n"),
    halt(0).
