#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_services -setcookie cvmfs

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main([MnesiaRootDir]) ->
    io:format("Node name: ~p.~n", [node()]),
    application:set_env(mnesia, dir, MnesiaRootDir),
    io:format("Mnesia root dir: ~p.~n", [MnesiaRootDir]),
    case mnesia:create_schema([node()]) of
        ok ->
            io:format("Mnesia schema created.~n");
        {error, Reason2} ->
            io:format("Mnesia schema could not be created. Reason: ~p.~n", [Reason2]),
            halt(1)
    end,
    halt(0).
