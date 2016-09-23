%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_auth).

-behaviour(gen_server).

%% API
-export([start_link/1, get_user_permissions/1
        ,add_user/2, remove_user/1, get_users/0
        ,add_repo/2, remove_repo/1, get_repos/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Records used as table entries
-record(repo_entry, {repo_id :: binary(), repo_path :: binary()}).
-record(acl_entry, {user_id :: binary(), repo_ids :: [binary()]}).

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
%% Returns a list of all repo names in the `repos` table.
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
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({RepoList, ACL}) ->
    ets:new(repos, [private, named_table, set, {keypos, #repo_entry.repo_id}]),
    ets:new(acl, [private, named_table, set, {keypos, #acl_entry.user_id}]),

    priv_populate_repos(RepoList),
    priv_populate_acl(ACL),

    cvmfs_om_log:info("CVMFS Auth storage module initialized."),
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
handle_call({auth_req, user_perms, User}, _From, _State) when is_binary(User) ->
    {reply, priv_get_user_paths(User), _State};
handle_call({auth_req, add_user, {User, Repos}}, _From, _State) ->
    {reply, priv_add_user(User, Repos), _State};
handle_call({auth_req, remove_user, User}, _From, _State) ->
    {reply, priv_remove_user(User), _State};
handle_call({auth_req, get_users}, _From, _State) ->
    {reply, priv_get_users(), _State};
handle_call({auth_req, add_repo, {Repo, Path}}, _From, _State) ->
    {reply, priv_add_repo(Repo, Path), _State};
handle_call({auth_req, remove_repo, Repo}, _From, _State) ->
    {reply, priv_remove_repo(Repo), _State};
handle_call({auth_req, get_repos}, _From, _State) ->
    {reply, priv_get_repos(), _State}.

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
handle_cast(_Msg, State) ->
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
handle_info(_Info, State) ->
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
terminate(_Reason, _State) ->
    ets:delete(acl),
    ets:delete(repos),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec priv_get_user_paths(binary()) -> {ok, [binary()]} | user_not_found.
priv_get_user_paths(User) when is_binary(User) ->
    case ets:lookup(acl, User) of
        [] ->
            user_not_found;
        AclEntries ->
                    {ok, [Path || #acl_entry{repo_ids = Repos} <- AclEntries,
                                  Repo <- Repos,
                                  #repo_entry{repo_path = Path} <- ets:lookup(repos, Repo)]}
    end.

priv_add_user(User, Repos) ->
    Status = ets:insert_new(acl, #acl_entry{user_id = User, repo_ids = Repos}),
    case Status of
        false ->
            user_already_exists;
        true ->
            ok
    end.

priv_remove_user(User) ->
    ets:delete(acl, User),
    ok.

priv_get_users() ->
    ets:foldl(fun(#acl_entry{user_id = User}, Acc) -> [User | Acc] end, [], acl).

priv_add_repo(Repo, Path) ->
    Status = ets:insert_new(repos, #repo_entry{repo_id = Repo, repo_path = Path}),
    case Status of
        false ->
            repo_already_exists;
        true ->
            ok
    end.

priv_remove_repo(Repo) ->
    ets:delete(repos, Repo),
    ok.

priv_get_repos() ->
    ets:foldl(fun(#repo_entry{repo_id = Repo}, Acc) -> [Repo | Acc] end, [], repos).

-spec priv_populate_acl([{binary(), [binary()]}]) -> [true].
priv_populate_acl(ACL) ->
    [ets:insert(acl, #acl_entry{user_id = ClientId,
                                repo_ids = RepoList}) || {ClientId, RepoList} <- ACL].

%% has side-effects. Will fill the ETS table 'repos'
-spec priv_populate_repos([{binary(), binary()}]) -> [true].
priv_populate_repos(RepoList) ->
    [ets:insert(repos, #repo_entry{repo_id = RepoId,
                                   repo_path = RepoPath}) || {RepoId, RepoPath} <- RepoList].
