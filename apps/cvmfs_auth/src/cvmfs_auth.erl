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
        ,add_user/2, remove_user/1
        ,add_repo/2, remove_repo/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Records used as table entries
-record(repo_entry, {repo_id :: binary(), repo_path :: binary()}).
-record(acl_entry, {client_id :: binary(), repo_ids :: [binary()]}).

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

add_user(_User, _Repos) ->
    ok.

remove_user(_User) ->
    ok.

add_repo(_Repo, _Paths) ->
    ok.

remove_repo(_Repo) ->
    ok.

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
    ets:new(acl, [private, named_table, set, {keypos, #acl_entry.client_id}]),

    populate_repos(RepoList),
    populate_acl(ACL),

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
    case get_user_paths(User) of
        user_not_found ->
            {reply, user_not_found, _State};
        {ok, Paths} when is_list(Paths) ->
            {reply, {ok, Paths}, _State}
    end.

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

-spec get_user_paths(binary()) -> {ok, [binary()]} | user_not_found.
get_user_paths(User) when is_binary(User) ->
    case ets:lookup(acl, User) of
        [] ->
            user_not_found;
        AclEntries ->
                    {ok, [Path || #acl_entry{repo_ids = Repos} <- AclEntries,
                                  Repo <- Repos,
                                  #repo_entry{repo_path = Path} <- ets:lookup(repos, Repo)]}
    end.

-spec populate_acl([{binary(), [binary()]}]) -> [true].
populate_acl(ACL) ->
    [ets:insert(acl, #acl_entry{client_id = ClientId,
                                repo_ids = RepoList}) || {ClientId, RepoList} <- ACL].

%% has side-effects. Will fill the ETS table 'repos'
-spec populate_repos([{binary(), binary()}]) -> [true].
populate_repos(RepoList) ->
    [ets:insert(repos, #repo_entry{repo_id = RepoId,
                                   repo_path = RepoPath}) || {RepoId, RepoPath} <- RepoList].
