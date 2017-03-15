%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_auth public API
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/1
        ,check_keyid_for_repo/2
        ,add_key/2, remove_key/1
        ,add_repo/2, remove_repo/1, get_repos/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Records

-record(repo, {
          % the FQDN of the repository
          name :: binary(),
          % public id of the keys registered to modify this repo
          key_ids :: [binary()]}).

-record(key, {
          key_id :: binary(),
          secret :: binary()}).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(Args) -> {ok, Pid} | ignore | {error, Error}
                              when Args :: term(), Pid :: pid(),
                                   Error :: {already_start, pid()} | term().
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).


-spec check_keyid_for_repo(KeyId, Repo) -> {ok, Allowed} | {error, invalid_path}
                                        when KeyId :: binary(),
                                             Repo :: binary(),
                                             Allowed :: boolean().
check_keyid_for_repo(KeyId, Repo) ->
    gen_server:call(?MODULE, {auth_req, check_keyid_for_repo, {KeyId, Repo}}).


-spec add_key(KeyId :: binary(), Secret :: binary())-> ok.
add_key(KeyId, Secret) ->
    gen_server:call(?MODULE, {auth_req, add_key, {KeyId, Secret}}).


-spec remove_key(KeyId :: binary()) -> ok.
remove_key(KeyId) ->
    gen_server:call(?MODULE, {auth_req, remove_key, KeyId}).


-spec add_repo(Repo :: binary(), KeyIds :: [binary()]) -> ok.
add_repo(Repo, KeyIds) ->
    gen_server:call(?MODULE, {auth_req, add_repo, {Repo, KeyIds}}).


-spec remove_repo(Repo :: binary()) -> ok.
remove_repo(Repo) ->
    gen_server:call(?MODULE, {auth_req, remove_repo, Repo}).


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all repo names in the `repo` table.
%% Potentially expensive!
%%
%% @end
%%--------------------------------------------------------------------
-spec get_repos() -> Repos :: [binary()].
get_repos() ->
    gen_server:call(?MODULE, {auth_req, get_repos}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% Arguments:
%%   RepoList - list of managed repositories
%%   Keys - key list ( [{keyid, secret}] )
%%
%% @end
%%--------------------------------------------------------------------
init({RepoList, Keys}) ->
    {ok, MnesiaSchemaLocation} = application:get_env(mnesia, schema_location),
    AllNodes = [node() | nodes()],
    CopyMode = case MnesiaSchemaLocation of
                   disc ->
                       {disc_copies, AllNodes};
                   ram ->
                       {ram_copies, AllNodes}
               end,
    mnesia:create_table(repo, [CopyMode
                              ,{type, set}
                              ,{attributes, record_info(fields, repo)}]),
    ok = mnesia:wait_for_tables([repo], 10000),
    p_populate_repos(RepoList),
    lager:info("Repository list initialized."),

    mnesia:create_table(key, [CopyMode
                             ,{type, set}
                             ,{attributes, record_info(fields, key)}]),
    ok = mnesia:wait_for_tables([key], 10000),
    p_populate_keys(Keys),
    lager:info("Key list initialized."),

    lager:info("Auth module initialized."),
    {ok, []}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({auth_req, check_keyid_for_repo, {KeyId, Repo}}, _From, State) ->
    Reply = p_check_keyid_for_repo(KeyId, Repo),
    lager:info("Request received: {check_keyid_for_repo, {~p, ~p}} -> Reply: ~p", [KeyId, Repo, Reply]),
    {reply, Reply, State};
handle_call({auth_req, add_key, {KeyId, Secret}}, _From, State) ->
    Reply = p_add_key(KeyId, Secret),
    lager:info("Request received: {add_key, {~p}} -> Reply: ~p", [KeyId, Reply]),
    {reply, Reply, State};
handle_call({auth_req, remove_key, Key}, _From, State) ->
    Reply = p_remove_key(Key),
    lager:info("Request received: {remove_key, ~p} -> Reply: ~p", [Key, Reply]),
    {reply, Reply, State};
handle_call({auth_req, add_repo, {Repo, KeyIds}}, _From, State) ->
    Reply = p_add_repo(Repo, KeyIds),
    lager:info("Request received: {add_repo, {~p, ~p}} -> Reply: ~p", [Repo, KeyIds, Reply]),
    {reply, Reply, State};
handle_call({auth_req, remove_repo, Repo}, _From, State) ->
    Reply = p_remove_repo(Repo),
    lager:info("Request received: {remove_repo, ~p} -> Reply: ~p", [Repo, Reply]),
    {reply, Reply, State};
handle_call({auth_req, get_repos}, _From, State) ->
    Reply = p_get_repos(),
    lager:info("Request received: {get_repos} -> Reply: ~p", [Reply]),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    lager:info("Cast received: ~p -> noreply", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    lager:warning("Unknown message received: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
terminate(Reason, _State) ->
    lager:info("Terminating with reason: ~p", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
code_change(OldVsn, State, _Extra) ->
    lager:info("Code change request received. Old version: ~p", [OldVsn]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec p_check_keyid_for_repo(KeyId, Repo) -> {ok, Allowed} | {error, invalid_path}
                                   when KeyId :: binary(),
                                        Repo :: binary(),
                                        Allowed :: boolean().
p_check_keyid_for_repo(KeyId, Repo) ->
    T1 = fun() ->
                 case mnesia:read(repo, Repo) of
                     [#repo{key_ids = KeyIds} | _] ->
                         {ok, lists:member(KeyId, KeyIds)};
                     _ ->
                         {error, invalid_path}
                 end
         end,
    {atomic, Result} = mnesia:transaction(T1),
    Result.


-spec p_add_key(KeyId :: binary(), Secret :: binary()) -> ok.
p_add_key(KeyId, Secret) ->
    T = fun() ->
                mnesia:write(#key{key_id = KeyId, secret = Secret})
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_remove_key(Key :: binary()) -> ok.
p_remove_key(Key) ->
    T = fun() ->
                mnesia:delete({key, Key})
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_add_repo(Repo :: binary(), KeyIds :: [binary()]) -> ok.
p_add_repo(Repo, KeyIds) ->
    T = fun() ->
                mnesia:write(#repo{name = Repo, key_ids = KeyIds})
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_remove_repo(Repo :: binary()) -> ok.
p_remove_repo(Repo) ->
    T = fun() ->
                mnesia:delete({repo, Repo})
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_get_repos() -> Repos :: [binary()].
p_get_repos() ->
    T = fun() ->
                mnesia:foldl(fun(#repo{name = Repo}, Acc) -> [Repo | Acc] end, [], repo)
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.


-spec p_populate_keys(Keys :: [{KeyId :: binary, Secret :: binary()}]) -> boolean().
p_populate_keys(Keys) ->
    T = fun() ->
                lists:all(fun(V) -> V =:= ok end,
                          [mnesia:write(#key{key_id = KeyId, secret = Secret}) || {KeyId, Secret} <- Keys])
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_populate_repos(RepoList :: [{Repo :: binary(), Fqdn :: binary()}]) -> boolean().
p_populate_repos(RepoList) ->
    T = fun() ->
                lists:all(fun(V) -> V =:= ok end,
                          [mnesia:write(#repo{name = Repo, key_ids = KeyIds}) || {Repo, KeyIds} <- RepoList])
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.
