%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_auth public API
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth_util).

-export([compute_hmac/2]).


%%--------------------------------------------------------------------
%% @doc
%% Computes the HMAC of a Message with Secret, using the SHA1 hash
%% function. The output is a hexstring, compatible with the HMACs
%% computed by CVMFS.
%% @end
%%--------------------------------------------------------------------
-spec compute_hmac(Secret :: binary(), Message :: binary()) -> binary().
compute_hmac(Secret, Message) ->
    base64:encode(integer_to_hexstring(crypto:hmac(sha, Secret, Message))).


%%--------------------------------------------------------------------
%% @doc
%% Converts an arbitrary sized integer to a hexstring.
%% @end
%%--------------------------------------------------------------------
integer_to_hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X]));
integer_to_hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X]));
integer_to_hexstring(<<X:256/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~64.16.0b", [X]));
integer_to_hexstring(<<X:512/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~128.16.0b", [X])).

