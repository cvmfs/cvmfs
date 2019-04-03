#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_gateway -setcookie cvmfs

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main([MnesiaRootDir]) ->
    application:set_env(mnesia, dir, MnesiaRootDir),
    case mnesia:create_schema([node()]) of
        ok ->
            ok;
        {error, Reason2} ->
            io:format("Mnesia schema could not be created. Reason: ~p.~n", [Reason2]),
            halt(1)
    end,
    halt(0).
