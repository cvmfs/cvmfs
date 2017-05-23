#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname cvmfs_services -setcookie cvmfs -mnesia dir '/opt/cvmfs_mnesia'

%%% -------------------------------------------------------------------
%%%
%%%  This file is part of the CernVM File System.
%%%
%%% -------------------------------------------------------------------

main(_) ->
    io:format("Setting up Mnesia~n"),
    io:format("Node name: ~p~n", [node()]),
    application:load(mnesia),
    ok = file:make_dir("/opt/cvmfs_mnesia"),
    Ret = mnesia:create_schema([node()]),
    io:format("Mnesia schema: ~p~n", [Ret]).
