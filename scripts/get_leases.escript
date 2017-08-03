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
    MainNodeAtom = list_to_atom("cvmfs_services@" ++ net_adm:localhost()),
    Leases = rpc:call(MainNodeAtom, cvmfs_lease, get_leases, []),
    io:format("Active leases: ~p~n", [Leases]),
    halt(0).
