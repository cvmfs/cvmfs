#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_services -setcookie cvmfs -mnesia dir '/opt/cvmfs_mnesia'

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main(_) ->
    CvmfsMnesiaRoot = "/opt/cvmfs_mnesia",
    io:format("Setting up Mnesia~n"),
    io:format("Node name: ~p.~n", [node()]),
    application:load(mnesia),
    case file:make_dir(CvmfsMnesiaRoot) of
        ok ->
            io:format("~p created.~n", [CvmfsMnesiaRoot]);
        {error, eexist} ->
            io:format("~p exists. Continuing.~n", [CvmfsMnesiaRoot]);
        {error, Reason1} ->
            io:foramt("Could not create ~p. Reason: ~p.~n", [CvmfsMnesiaRoot, Reason1]),
            halt(1)
    end,
    case mnesia:create_schema([node()]) of
        ok ->
            io:format("Mnesia schema created.~n");
        {error, Reason2} ->
            io:format("Mnesia schema could not be created. Reason: ~p.~n", [Reason2]),
            halt(1)
    end,
    halt(0).
