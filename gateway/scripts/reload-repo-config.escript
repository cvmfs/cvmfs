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
    io:format("Reloading repository configuration... "),
    [HostName | _] = string:split(net_adm:localhost(), "."),
    MainNodeAtom = list_to_atom("cvmfs_gateway@" ++ HostName),
    ok = rpc:call(MainNodeAtom, cvmfs_auth, reload_repo_config, []),
    io:format("done.~n"),
    halt(0).
