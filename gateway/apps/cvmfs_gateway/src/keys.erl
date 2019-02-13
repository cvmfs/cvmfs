%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to gateway keys
%%% @end
%%%-------------------------------------------------------------------

-module(keys).

-export([parse_file/1,
         parse_binary/1]).

-spec parse_file(FileName :: list() | binary()) -> {KeyType :: binary(),
                                                    KeyId :: binary(),
                                                    Secret :: binary()}.
parse_file(FileName) ->
    {ok, Body} = file:read_file(FileName),
    parse_binary(Body).


%%--------------------------------------------------------------------
%% @doc
%% Parses a binary containing the description of a gateway key
%%
%% Expects a binary as input with the following structure
%% <<"plain_text <KEY_ID> <SECRET>">>
%%
%% @end
%%--------------------------------------------------------------------
-spec parse_binary(Data :: binary()) -> {KeyType :: binary(),
                                         KeyId :: binary(),
                                         Secret :: binary()}.
parse_binary(Data) ->
    [Line | _] = [L || L <- binary:split(Data, <<"\n">>), L =/= <<>>],
    ReplacedTabs = re:replace(Line, <<"\t">>, <<" ">>, [global, {return, binary}]),
    SplitAtWhitespace = binary:split(ReplacedTabs, <<" ">>, [global]),
    RemovedEmpty = lists:filter(fun(V) -> V =/= <<"">> end, SplitAtWhitespace),
    [KeyType, KeyId, Secret] = RemovedEmpty,
    {KeyType, KeyId, Secret}.


