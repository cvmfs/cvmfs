%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc helper functions related to gateway keys
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_keys).

-include_lib("eunit/include/eunit.hrl").

-export([parse_file/1,
         parse_binary/1]).

-spec parse_file(FileName) -> {ok, KeyType, KeyId, Secret} | {error, Reason}
    when FileName :: list() | binary(),
         KeyType :: binary(),
         KeyId :: binary(),
         Secret :: binary(),
         Reason :: atom() | {invalid_key_file, binary()}.
parse_file(FileName) ->
    case file:read_file(FileName) of
        {ok, Body} ->
            case parse_binary(Body) of
                {error, invalid_format} ->
                    {error, {invalid_key_file, FileName}};
                ParseResults ->
                    ParseResults
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Parses a binary containing the description of a gateway key
%%
%% Expects a binary as input with the following structure
%% <<"plain_text <KEY_ID> <SECRET>">>
%%
%% @end
%%--------------------------------------------------------------------
-spec parse_binary(Data) -> {ok, KeyType, KeyId, Secret} | {error, invalid_format}
    when Data :: binary(),
         KeyType :: binary(),
         KeyId :: binary(),
         Secret :: binary().
parse_binary(Data) ->
    [Line | _] = [L || L <- binary:split(Data, <<"\n">>), L =/= <<>>],
    ReplacedTabs = re:replace(Line, <<"\t">>, <<" ">>, [global, {return, binary}]),
    SplitAtWhitespace = binary:split(ReplacedTabs, <<" ">>, [global]),
    RemovedEmpty = lists:filter(fun(V) -> V =/= <<"">> end, SplitAtWhitespace),
    case RemovedEmpty of
        [<<"plain_text">>, KeyId, Secret] ->
            {ok, <<"plain_text">>, KeyId, Secret};
        _ ->
            {error, invalid_format}
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%    Tests    %%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%



valid_key_files_test() ->
    Data = [<<"plain_text id secret">>,
            <<"    plain_text   id   secret    ">>,
            <<"\tplain_text\tid\tsecret\t">>,
            <<" \t   plain_text \t  id \t  secret \t \n  ">>],
    [?assert({ok, <<"plain_text">>, <<"id">>, <<"secret">>} =:= cvmfs_keys:parse_binary(D)) || D <- Data].

invalid_key_files_test() ->
    Data = [<<"plane_text id secret">>,
            <<"plain_textid secret">>,
            <<"plain_text id secret garbage">>],
    [?assert({error, invalid_format} =:= cvmfs_keys:parse_binary(D)) || D <- Data].
