%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease).

-compile([{parse_transform, lager_transform}]).

-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, stop/0
        ,request_lease/4, end_lease/1
        ,check_lease/2
        ,get_leases/0, clear_leases/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% Records used as table entries

-record(lease, { s_path :: binary()   % subpath which is locked
               , u_id   :: binary()   % user identifier
               , s_pub  :: binary()   % public string used for token generation
               , s_sec  :: binary()   % secret used for token generation
               , time   :: integer()  % timestamp (time when lease acquired)
               }).


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

%%--------------------------------------------------------------------
%% @doc
%% Stops the server (only useful without a supervision tree)
%%
%% @spec stop() -> ok
%% @end
%%--------------------------------------------------------------------
stop() ->
    gen_server:cast(?MODULE, stop).

%%--------------------------------------------------------------------
%% @doc
%% Requests a new lease
%%
%% @spec request_lease(User, Path)) -> {ok, LeaseId} | {busy, TimeRemaining}
%% @end
%%--------------------------------------------------------------------
-spec request_lease(User, Path, Public, Secret) ->
                           {ok, LeaseId} | {busy, TimeRemaining}
                                       when User :: binary(),
                                            Path :: binary(),
                                            Public :: binary(),
                                            Secret :: binary(),
                                            LeaseId :: binary(),
                                            TimeRemaining :: integer().
request_lease(User, Path, Public, Secret) ->
    gen_server:call(?MODULE, {lease_req, new_lease, {User, Path, Public, Secret}}).

%%--------------------------------------------------------------------
%% @doc
%% Gives up an existing lease
%%
%% @spec end_lease(Path) -> ok | {error, lease_not_found}
%% @end
%%--------------------------------------------------------------------
end_lease(Path) ->
    gen_server:call(?MODULE, {lease_req, end_lease, Path}).

%%--------------------------------------------------------------------
%% @doc
%% Checks the validity of a lease
%%
%% @spec check_lease(User, Path) -> ok | {error, invalid_lease}
%% @end
%%--------------------------------------------------------------------
-spec check_lease(User, Path) -> ok | {error, Reason}
                                     when User :: binary(),
                                          Path :: binary(),
                                          Reason :: lease_not_found |
                                                    lease_expired.
check_lease(User, Path) ->
    gen_server:call(?MODULE, {lease_req, check_lease, {User, Path}}).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all active leases
%%
%% @spec get_leases() -> Leases
%% @end
%%--------------------------------------------------------------------
-spec get_leases() -> [#lease{}].
get_leases() ->
    gen_server:call(?MODULE, {lease_req, get_leases}).

%%--------------------------------------------------------------------
%% @doc
%% Clears all existing leases from the table.
%%
%% @spec clear_leases() -> ok.
%% @end
%%--------------------------------------------------------------------
-spec clear_leases() -> ok.
clear_leases() ->
    gen_server:call(?MODULE, {lease_req, clear_leases}).


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
init(_) ->
    {ok, MnesiaSchemaLocation} = application:get_env(mnesia, schema_location),
    AllNodes = [node() | nodes()],
    CopyMode = case MnesiaSchemaLocation of
                   disc ->
                       {disc_copies, AllNodes};
                   ram ->
                       {ram_copies, AllNodes}
               end,
    mnesia:create_table(lease, [CopyMode
                               ,{type, set}
                               ,{attributes, record_info(fields, lease)}
                               ,{index, [s_pub]}]),
    ok = mnesia:wait_for_tables([lease], 10000),
    lager:info("Lease table initialized"),
    {ok, {}}.

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
handle_call({lease_req, new_lease, {User, Path, Public, Secret}}, _From, State) ->
    Reply = priv_new_lease(User, Path, Public, Secret, State),
    lager:info("Request received: {new_lease, ~p} -> Reply: ~p"
              ,[{User, Path}, Reply]),
    {reply, Reply, State};
handle_call({lease_req, end_lease, Path}, _From, State) ->
    Reply = priv_end_lease(Path),
    lager:info("Request received: {end_lease, ~p} -> Reply: ~p"
              ,[Path, Reply]),
    {reply, Reply, State};
handle_call({lease_req, check_lease, {User, Path}}, _From, State) ->
    Reply = priv_check_lease(User, Path),
    lager:info("Request received: {check_lease, ~p} -> Reply: ~p"
              ,[{User, Path}, Reply]),
    {reply, Reply, State};
handle_call({lease_req, get_leases}, _From, State) ->
    Reply = priv_get_leases(),
    lager:info("Request received: {get_leases} -> Reply: ~p"
              ,[Reply]),
    {reply, Reply, State};
handle_call({lease_req, clear_leases}, _From, State) ->
    Reply = priv_clear_leases(),
    lager:info("Request received: {clear_leases} -> Reply: ~p"
              ,[Reply]),
    {reply, Reply, State};
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
handle_cast(stop, State) ->
    lager:info("Cast received: stop"),
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
    lager:info("Terminating with reason: ~p", [Reason]),
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

priv_new_lease(User, Path, Public, Secret, _State) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_lease, max_lease_time),

    %% Match statement that selects all rows with a given repo,
    %% returning a list of {Path, Time} pairs
    MS = ets:fun2ms(fun(#lease{u_id = U, time = T, s_path = P}) ->
                            {P, T}
                    end),

    T = fun() ->
                CurrentTime = erlang:system_time(milli_seconds),

                %% We select the rows related to a given repository
                Results = mnesia:select(lease, MS),

                %% We filter out entries which don't overlap with Path
                case lists:filter(fun({P, _}) ->
                                          cvmfs_lease_path_util:are_overlapping(P, Path)
                                  end,
                                  Results) of
                    %% An everlapping path was found
                    [{P, Time} | _] ->
                        RemainingTime = MaxLeaseTime - (CurrentTime - Time),
                        case RemainingTime > 0 of
                            %% The old lease is still valid, return busy message
                            true ->
                                {busy, RemainingTime};
                            %% The old lease is expired. Delete it and insert the new one
                            false ->
                                mnesia:delete({lease, P}),
                                priv_write_row(User, Path, Public, Secret)
                        end;
                    %% No overlapping paths were found; just insert the new entry
                    _ ->
                        priv_write_row(User, Path, Public, Secret)
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.

priv_check_lease(User, Path) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_lease, max_lease_time),
    T = fun() ->
                CurrentTime = erlang:system_time(milli_seconds),
                case mnesia:read(lease, Path) of
                    [] ->
                        {error, lease_not_found};
                    [#lease{s_path = Path, u_id = User, time = Time} | _]  ->
                        RemainingTime = MaxLeaseTime - (CurrentTime - Time),
                        case RemainingTime > 0 of
                            true ->
                                ok;
                            false ->
                                {error, lease_expired}
                        end
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.

priv_end_lease(Path) ->
    T = fun() ->
                case mnesia:read(lease, Path) of
                    [] ->
                        {error, lease_not_found};
                    _ ->
                        mnesia:delete({lease, Path})
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.

priv_get_leases() ->
    T = fun() ->
                mnesia:foldl(fun(Lease, Acc) -> [Lease | Acc] end, [], lease)
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.

priv_clear_leases() ->
    {atomic, Result} = mnesia:clear_table(lease),
    Result.

priv_write_row(User, Path, Public, Secret) ->
    mnesia:write(#lease{s_path = Path,
                        u_id = User,
                        s_pub = Public,
                        s_sec = Secret,
                        time = erlang:system_time(milli_seconds)}).
