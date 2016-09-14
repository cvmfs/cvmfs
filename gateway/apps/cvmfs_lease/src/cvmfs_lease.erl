%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    {ok, #state{}}.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
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

%% %%--------------------------------------------------------------------
%% %% @doc Returns the list of repos for which ClientId can obtain leases
%% %% @end
%% %%--------------------------------------------------------------------
%% -spec get_client_acl(ClientId :: string()) -> no_acl_entry | {ok, [string()]}.
%% get_client_acl(ClientId) when is_list(ClientId) ->
%%     case ets:lookup(acl, ClientId) of
%%         [] ->
%%             cmvfs_om_log:debug("ACL entry not found for client id ~p", [ClientId]),
%%             no_acl_entry;
%%         [#acl_entry{client_id = ClientId, repo_ids = Repos} | _ ] ->
%%             {ok, Repos}
%%     end.

%% %%--------------------------------------------------------------------
%% %% @doc Inserts a new lease into the 'leases' table
%% %% @end
%% %%--------------------------------------------------------------------
%% -spec insert_lease(RepoPath ::string(), ClientId :: string(), SessionId :: string()) -> true.
%% insert_lease(RepoPath, ClientId, SessionId) ->
%%     ets:insert(leases, #lease_entry{repo_path = RepoPath,
%%                                     client_id = ClientId,
%%                                     session_id = SessionId,
%%                                     timestamp = erlang:monotonic_time(seconds)}).

%% %%--------------------------------------------------------------------
%% %% @doc Deletes a lease from the 'leases' table
%% %% @end
%% %%--------------------------------------------------------------------
%% -spec delete_lease(RepoPath :: string()) -> true.
%% delete_lease(RepoPath) ->
%%     ets:delete(leases, RepoPath).

%% %%--------------------------------------------------------------------
%% %% @doc Returns a list of pairs {Path, Timestamp} representing all the
%% %%      active leases
%% %% @end
%% %%--------------------------------------------------------------------
%% get_leases_for_path(Path) ->
%%     [].
