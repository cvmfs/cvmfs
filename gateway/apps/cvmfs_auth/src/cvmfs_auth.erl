%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/1, stop/0
        ,get_user_permissions/1
        ,add_user/2, remove_user/1, get_users/0
        ,add_repo/2, remove_repo/1, get_repos/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Records used as table entries

-record(repo, { r_id :: binary() % repo identifier
              , path :: binary() % the path of the repository
              }).

%% u_id - user identifier
%% r_ids - identifiers of repos to which user can make changes
-record(acl, {u_id :: binary(), r_ids :: [binary()]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

stop() ->
    gen_server:cast(?MODULE, stop).


-spec get_user_permissions(binary()) -> user_not_found | {ok, [binary()]}.
get_user_permissions(User) when is_binary(User) ->
    gen_server:call(?MODULE, {auth_req, user_perms, User}).

-spec add_user(binary(), [binary()]) -> user_already_exists | ok.
add_user(User, [H | _] = Repos) when is_binary(User), is_list(Repos), is_binary(H) ->
    gen_server:call(?MODULE, {auth_req, add_user, {User, Repos}}).

-spec remove_user(binary()) -> ok.
remove_user(User) when is_binary(User) ->
    gen_server:call(?MODULE, {auth_req, remove_user, User}).


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all usernames in the `acl` table.
%% Potentially expensive!
%%
%% @spec spec get_users() -> [binary()]
%% @end
%%--------------------------------------------------------------------
-spec get_users() -> [binary()].
get_users() ->
    gen_server:call(?MODULE, {auth_req, get_users}).

-spec add_repo(binary(), binary()) -> repo_already_exists | ok.
add_repo(Repo, Path) when is_binary(Repo), is_binary(Path) ->
    gen_server:call(?MODULE, {auth_req, add_repo, {Repo, Path}}).

-spec remove_repo(binary()) -> ok.
remove_repo(Repo) when is_binary(Repo) ->
    gen_server:call(?MODULE, {auth_req, remove_repo, Repo}).

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all repo names in the `repo` table.
%% Potentially expensive!
%%
%% @spec spec get_users() -> [binary()]
%% @end
%%--------------------------------------------------------------------
-spec get_repos() -> [binary()].
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
%%   ACL - access control list ([{username, [repo_name]}])
%%   MnesiaSchema - mnesia database schema. 'disc_copies' for normal use,
%%                  'ram_copies' for the test suite.
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({RepoList, ACL, MnesiaSchema}) ->
    mnesia:create_table(repo, [{MnesiaSchema, [node() | nodes()]}
                              ,{type, set}
                              ,{attributes, record_info(fields, repo)}]),
    ok = mnesia:wait_for_tables([repo], 10000),
    priv_populate_repos(RepoList),
    lager:info("Repository list initialized."),

    mnesia:create_table(acl, [{MnesiaSchema, [node() | nodes()]}
                             ,{type, set}
                             ,{attributes, record_info(fields, acl)}]),
    ok = mnesia:wait_for_tables([acl], 10000),
    priv_populate_acl(ACL),
    lager:info("Access control list initialized."),

    lager:info("Auth module initialized."),
    {ok, []}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({auth_req, user_perms, User}, _From, State) when is_binary(User) ->
    Reply = priv_get_user_paths(User),
    lager:info("Request received: {user_perms, ~p} -> Reply: ~p", [User, Reply]),
    {reply, Reply, State};
handle_call({auth_req, add_user, {User, Repos}}, _From, State) ->
    Reply = priv_add_user(User, Repos),
    lager:info("Request received: {add_user, {~p, ~p}} -> Reply: ~p", [User, Repos, Reply]),
    {reply, Reply, State};
handle_call({auth_req, remove_user, User}, _From, State) ->
    Reply = priv_remove_user(User),
    lager:info("Request received: {remove_user, ~p} -> Reply: ~p", [User, Reply]),
    {reply, Reply, State};
handle_call({auth_req, get_users}, _From, State) ->
    Reply = priv_get_users(),
    lager:info("Request received: {get_users} -> Reply: ~p", [Reply]),
    {reply, Reply, State};
handle_call({auth_req, add_repo, {Repo, Path}}, _From, State) ->
    Reply = priv_add_repo(Repo, Path),
    lager:info("Request received: {add_repo, {~p, ~p}} -> Reply: ~p", [Repo, Path, Reply]),
    {reply, Reply, State};
handle_call({auth_req, remove_repo, Repo}, _From, State) ->
    Reply = priv_remove_repo(Repo),
    lager:info("Request received: {remove_repo, ~p} -> Reply: ~p", [Repo, Reply]),
    {reply, Reply, State};
handle_call({auth_req, get_repos}, _From, State) ->
    Reply = priv_get_repos(),
    lager:info("Request received: {get_repos} -> Reply: ~p", [Reply]),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    lager:info("Request received: stop"),
    {stop, normal, State};
handle_cast(Msg, State) ->
    lager:info("Cast received: ~p -> noreply", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
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
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, _State) ->
    lager:info("CVMFS auth module terminating with reason: ~p", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(OldVsn, State, _Extra) ->
    lager:info("Code change request received. Old version: ~p", [OldVsn]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec priv_get_user_paths(binary()) -> {ok, [binary()]} | user_not_found.
priv_get_user_paths(User) when is_binary(User) ->
    T1 = fun() ->
                case mnesia:read(acl, User) of
                    [] ->
                        user_not_found;
                    AclEntries ->
                        T2 = fun() ->
                                     {ok, [Path || #acl{r_ids = Repos} <- AclEntries,
                                                   Repo <- Repos,
                                                   #repo{path = Path} <- mnesia:read(repo, Repo)]}
                             end,
                        {atomic, Result} = mnesia:transaction(T2),
                        Result
                end
        end,
    {atomic, Result} = mnesia:transaction(T1),
    Result.

priv_add_user(User, Repos) ->
    T = fun() ->
                mnesia:write(#acl{u_id = User, r_ids = Repos})
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

priv_remove_user(User) ->
    T = fun() ->
                mnesia:delete({acl, User})
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

priv_get_users() ->
    T = fun() ->
                mnesia:foldl(fun(#acl{u_id = User}, Acc) -> [User | Acc] end, [], acl)
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

priv_add_repo(Repo, Path) ->
    T = fun() ->
                mnesia:write(#repo{r_id = Repo, path = Path})
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

priv_remove_repo(Repo) ->
    T = fun() ->
                mnesia:delete({repo, Repo})
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

priv_get_repos() ->
    T = fun() ->
                mnesia:foldl(fun(#repo{r_id = Repo}, Acc) -> [Repo | Acc] end, [], repo)
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

-spec priv_populate_acl([{binary(), [binary()]}]) -> [true].
priv_populate_acl(ACL) ->
    T = fun() ->
                [mnesia:write(#acl{u_id = ClientId, r_ids = RepoList}) || {ClientId, RepoList} <- ACL]
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.

-spec priv_populate_repos([{binary(), binary()}]) -> [true].
priv_populate_repos(RepoList) ->
    T = fun() ->
                [mnesia:write(#repo{r_id = RepoId, path = RepoPath}) || {RepoId, RepoPath} <- RepoList]
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.
