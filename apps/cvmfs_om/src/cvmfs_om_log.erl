%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease storage module
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_om_log).

-compile([{parse_transform, lager_transform}]).

-export([debug/1, info/1, notice/1, warning/1, error/1, critical/1, alert/1, emergency/1]).
-export([debug/2, info/2, notice/2, warning/2, error/2, critical/2, alert/2, emergency/2]).

%% Logging functions

debug(Fmt) ->
    lager:debug(Fmt, []).
debug(Fmt, Args) ->
    lager:debug(Fmt, Args).

info(Fmt) ->
    lager:info(Fmt, []).
info(Fmt, Args) ->
    lager:info(Fmt, Args).

notice(Fmt) ->
    lager:notice(Fmt, []).
notice(Fmt, Args) ->
    lager:notice(Fmt, Args).

warning(Fmt) ->
    lager:warning(Fmt, []).
warning(Fmt, Args) ->
    lager:warning(Fmt, Args).

error(Fmt) ->
    lager:error(Fmt, []).
error(Fmt, Args) ->
    lager:error(Fmt, Args).

critical(Fmt) ->
    lager:critical(Fmt, []).
critical(Fmt, Args) ->
    lager:critical(Fmt, Args).

alert(Fmt) ->
    lager:alert(Fmt, []).
alert(Fmt, Args) ->
    lager:alert(Fmt, Args).

emergency(Fmt) ->
    lager:emergency(Fmt, []).
emergency(Fmt, Args) ->
    lager:emergency(Fmt, Args).
