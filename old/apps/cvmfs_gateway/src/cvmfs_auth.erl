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
-export([start_link/0
        ,check_key_for_repo_path/3
        ,get_repos/0
        ,check_hmac/3
        ,reload_repo_config/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid} | ignore | {error, Error}
                          when Pid :: pid(),
                               Error :: {already_start, pid()} | term().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-spec check_key_for_repo_path(KeyId, Repo, Path) -> ok | {error,
                                                          invalid_repo |
                                                          invalid_path |
                                                          invalid_key}
                                                        when KeyId :: binary(),
                                                             Repo :: binary(),
                                                             Path :: binary().
check_key_for_repo_path(KeyId, Repo, Path) ->
    gen_server:call(?MODULE, {auth_req, check_key_for_repo_path, {KeyId, Repo, Path}}).


-spec get_repos() -> Repos :: [{binary(), [binary() | #{atom() => binary()}]}].
get_repos() ->
    gen_server:call(?MODULE, {auth_req, get_repos}).


-spec check_hmac(Message, KeyId, HMAC) -> boolean()
                                              when Message :: binary(),
                                                   KeyId :: binary(),
                                                   HMAC :: binary().
check_hmac(Message, KeyId, HMAC) ->
    gen_server:call(?MODULE, {auth_req, check_hmac, {Message, KeyId, HMAC}}).


-spec reload_repo_config() -> {ok, CfgVer} | {error, Reason}
                                  when CfgVer :: integer(), Reason :: term().
reload_repo_config() ->
    gen_server:call(?MODULE, {auth_req, reload_repo_config}).


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
init([]) ->
    ets:new(repos, [set, named_table, private]),
    ets:new(keys, [set, named_table, private]),

    case p_reload_repo_config() of
        {ok, CfgVer} ->
            lager:info("Repository configuration finished."),
            {ok, #{config_version => CfgVer}};
        {error, Reason} ->
            lager:error("Error in auth module initialization: ~p", [Reason]),
            {stop, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({auth_req, check_key_for_repo_path, {KeyId, Repo, Path}},
            _From, #{config_version := CfgVer} = State) ->
    Reply = p_check_key_for_repo_path(KeyId, Repo, Path, CfgVer),
    {reply, Reply, State};
handle_call({auth_req, get_repos}, _From, State) ->
    Reply = p_get_repos(),
    {reply, Reply, State};
handle_call({auth_req, check_hmac, {Message, KeyId, HMAC}}, _From, State) ->
    Reply = p_check_hmac(Message, KeyId, HMAC),
    {reply, Reply, State};
handle_call({auth_req, reload_repo_config}, _From, State) ->
    Reply = p_reload_repo_config(),
    case Reply of
        {ok, CfgVer} ->
            {reply, Reply, #{config_version => CfgVer}};
        _ ->
            {reply, Reply, State}
        end.


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

-spec p_check_key_for_repo_path(KeyId, Repo, Path, CfgVer) -> ok | {error,
                                                                    invalid_repo |
                                                                    invalid_key |
                                                                    invalid_path}
                                   when KeyId :: binary(),
                                        Repo :: binary(),
                                        Path :: binary(),
                                        CfgVer :: integer().
p_check_key_for_repo_path(KeyId, Repo, Path, CfgVer) ->
    case CfgVer of
        1 ->
            case ets:lookup(repos, Repo) of
                [{Repo, KeyIds} | _] ->
                    KeyValidForRepo = lists:member(KeyId, KeyIds),
                    case KeyValidForRepo of
                        true ->
                            case ets:lookup(keys, KeyId) of
                                [{_, _, AllowedPath} | _] ->
                                    Overlapping = cvmfs_path_util:are_overlapping(Path, AllowedPath),
                                    IsSubPath = size(Path) >= size(AllowedPath),
                                    case Overlapping and IsSubPath of
                                        true ->
                                            ok;
                                        false ->
                                            {error, invalid_path}
                                    end;
                                _ ->
                                    {error, invalid_path}
                            end;
                        false ->
                            {error, invalid_key}
                    end;
                _ ->
                    {error, invalid_repo}
            end;
        _ ->
            case ets:lookup(repos, Repo) of
                [{Repo, KeyIds} | _] ->
                    case lists:search(fun(#{id := Id}) -> Id =:= KeyId end, KeyIds) of
                        {value, #{path := AllowedPath}} ->
                            Overlapping = cvmfs_path_util:are_overlapping(Path, AllowedPath),
                            IsSubPath = size(Path) >= size(AllowedPath),
                            case Overlapping and IsSubPath of
                                true ->
                                    ok;
                                false ->
                                    {error, invalid_path}
                            end;
                        false ->
                            {error, invalid_key}
                    end;
                _ ->
                    {error, invalid_repo}
            end
    end.


-spec p_get_repos() -> Repos :: [{binary(), [binary() | #{atom() => binary()}]}].
p_get_repos() ->
    ets:foldl(fun({Repo, KeyIds}, Acc) -> [{Repo, KeyIds} | Acc] end, [], repos).


-spec p_check_hmac(Message :: binary(), KeyId :: binary(), HMAC :: binary()) -> boolean().
p_check_hmac(Message, KeyId, HMAC) ->
    Result = case ets:lookup(keys, KeyId) of
        [{KeyId, Secret} | _] ->
            {ok, Secret};
        [{KeyId, Secret, _} | _] ->
            {ok, Secret};
        _ ->
            error
    end,
    case Result of
        {ok, Sec} ->
            HMAC =:= cvmfs_auth_util:compute_hmac(Sec, Message);
        _ ->
            false
    end.


-spec p_reload_repo_config() -> {ok, CfgVer} | {error, Reason}
                                    when CfgVer :: integer(), Reason :: term().
p_reload_repo_config() ->
    Cfg = cvmfs_config:read(repo_config, cvmfs_config:default_repo_config()),

    CfgVer = maps:get(version, Cfg, 1),

    ets:delete_all_objects(repos),
    ets:delete_all_objects(keys),

    case cvmfs_config:load(Cfg) of
        {ok, Repos, Keys} ->
            case CfgVer of
                1 ->
                    populate_key_table_v1(Keys);
                _ ->
                    populate_key_table(Keys)
            end,
            populate_repo_table(Repos),
            {ok, CfgVer};
        {error, Reason} ->
            {error, Reason}
    end.

-spec populate_key_table(Keys :: [#{atom() := binary()}]) -> ok.
populate_key_table(Keys) ->
    lists:foreach(
        fun(#{id := Id, secret := Secret}) ->
            ets:insert(keys, {Id, Secret})
        end,
        Keys),
    ok.


-spec populate_key_table_v1(Keys :: [#{atom() := binary()}]) -> ok.
populate_key_table_v1(Keys) ->
    lists:foreach(
        fun(#{id := Id, secret := Secret, path := Path}) ->
            ets:insert(keys, {Id, Secret, Path})
        end,
        Keys),
    ok.


-spec populate_repo_table(RepoList :: [#{atom() => binary() | [binary()]}]) -> ok.
populate_repo_table(RepoList) ->
    [ets:insert(repos, {Repo, KeyIds}) || #{domain := Repo,
                                            keys := KeyIds} <- RepoList],
    ok.


