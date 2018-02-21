#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_gateway_ops -setcookie cvmfs

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main([]) ->
    io:format("Node name: ~p.~n", [node()]),
    [HostName | _] = string:split(net_adm:localhost(), "."),
    MainNodeAtom = list_to_atom("cvmfs_gateway@" ++ HostName),
    Leases = rpc:call(MainNodeAtom, cvmfs_lease, get_leases, []),
    io:format("Active leases: ~p~n", [Leases]),
    halt(0).
