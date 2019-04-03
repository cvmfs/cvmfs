%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_lease public API
%%%
%%% @end
%%%
%%%-------------------------------------------------------------------

-module(cvmfs_lease).

-compile([{parse_transform, lager_transform}]).

-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1
        ,request_lease/4, end_lease/1
        ,get_lease_secret/1, get_lease_path/1
        ,get_leases/0, clear_leases/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(lease, { path   :: {binary(), binary()}  % repository with subpath which is locked
               , key_id :: binary()  % user identifier
               , public :: binary()  % public string used for token generation
               , secret :: binary()  % secret used for token generation
               , time   :: integer() % timestamp (time when lease acquired)
               }).
-type lease() :: #lease{}.

%%%===================================================================
%%% Type specifications
%%%===================================================================
-type new_lease_result() :: ok | {busy, TimeRemaining :: integer()}.
-type lease_get_result() :: {ok, Lease :: lease()} |
                              {error, invalid_lease | lease_expired}.
-type lease_get_value() :: {ok, Value :: binary()} |
                              {error, invalid_lease | lease_expired}.

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
start_link(_) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Requests a new lease
%%
%% @end
%%--------------------------------------------------------------------
-spec request_lease(KeyId, Path, Public, Secret) -> new_lease_result()
                                                       when KeyId :: binary(),
                                                            Path :: binary(),
                                                            Public :: binary(),
                                                            Secret :: binary().
request_lease(KeyId, Path, Public, Secret) ->
    gen_server:call(?MODULE, {lease_req, new_lease, {KeyId, Path, Public, Secret}}).

%%--------------------------------------------------------------------
%% @doc
%% Gives up an existing lease
%%
%% @end
%%--------------------------------------------------------------------
-spec end_lease(Public :: binary()) -> ok.
end_lease(Public) ->
    gen_server:call(?MODULE, {lease_req, end_lease, Public}).

%%--------------------------------------------------------------------
%% @doc
%% Checks the validity of a lease
%%
%% @end
%%--------------------------------------------------------------------
-spec get_lease_secret(Public) -> lease_get_value()
                                 when Public :: binary().
get_lease_secret(Public) ->
    gen_server:call(?MODULE, {lease_req, get_lease_secret, Public}).


%%--------------------------------------------------------------------
%% @doc
%% Checks the validity of a lease
%%
%% @end
%%--------------------------------------------------------------------
-spec get_lease_path(Public) -> lease_get_value()
                                 when Public :: binary().
get_lease_path(Public) ->
    gen_server:call(?MODULE, {lease_req, get_lease_path, Public}).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of all active leases
%%
%% @end
%%--------------------------------------------------------------------
-spec get_leases() -> Leases :: [#lease{}].
get_leases() ->
    gen_server:call(?MODULE, {lease_req, get_all_leases}).

%%--------------------------------------------------------------------
%% @doc
%% Clears all existing leases from the table.
%%
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
                               ,{index, [public]}]),
    ok = mnesia:wait_for_tables([lease], 10000),
    lager:info("Lease table initialized"),

    lager:info("Lease module initialized."),
    {ok, {}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({lease_req, new_lease, {KeyId, Path, Public, Secret}}, _From, State) ->
    Reply = p_new_lease(KeyId, Path, Public, Secret, State),
    {reply, Reply, State};
handle_call({lease_req, end_lease, Public}, _From, State) ->
    Reply = p_end_lease(Public),
    {reply, Reply, State};
handle_call({lease_req, get_lease_secret, Public}, _From, State) ->
    Reply = case p_get_lease(Public) of
                {ok, #lease{secret = Secret}} ->
                    {ok, Secret};
                Other ->
                    Other
            end,
    {reply, Reply, State};
handle_call({lease_req, get_lease_path, Public}, _From, State) ->
    Reply = case p_get_lease(Public) of
                {ok, #lease{path = {Repo, <<>>}}} ->
                    {ok, <<Repo/binary, "/">>};
                {ok, #lease{path = {Repo, Path}}} ->
                    {ok, <<Repo/binary, "/", Path/binary>>};
                Other ->
                    Other
            end,
    {reply, Reply, State};
handle_call({lease_req, get_all_leases}, _From, State) ->
    Reply = p_get_leases(),
    {reply, Reply, State};
handle_call({lease_req, clear_leases}, _From, State) ->
    Reply = p_clear_leases(),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
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

-spec p_new_lease(KeyId, Path, Public, Secret, State) -> new_lease_result()
                                                            when KeyId :: binary(),
                                                                 Path :: binary(),
                                                                 Public :: binary(),
                                                                 Secret :: binary(),
                                                                 State :: map().
p_new_lease(KeyId, Path, Public, Secret, _State) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_gateway, max_lease_time),

    {Repo, Subpath} = cvmfs_path_util:split_repo_path(Path),

    %% Match statement that selects all rows with a given repo,
    %% returning a list of {Path, Time} pairs
    MS = ets:fun2ms(fun(#lease{path = {R, _}} = Lease) when R =:= Repo ->
                            Lease
                    end),

    AreOverlapping = fun(#lease{path = {_, P}}) ->
                             cvmfs_path_util:are_overlapping(P, Subpath)
                     end,

    T = fun() ->
                CurrentTime = erlang:system_time(milli_seconds),

                %% We select the rows related to a given repository
                %% We filter out entries which don't overlap with Path
                case lists:filter(AreOverlapping, mnesia:select(lease, MS)) of
                    %% An everlapping path was found
                    [#lease{path = ConflictingPath, time = Time} | _] ->
                        RemainingTime = MaxLeaseTime - (CurrentTime - Time),
                        case RemainingTime > 0 of
                            %% The old lease is still valid, return busy message
                            true ->
                                {busy, RemainingTime};
                            %% The old lease is expired. Delete it and insert the new one
                            false ->
                                mnesia:delete({lease, {Repo, ConflictingPath}}),
                                p_write_row(KeyId, Repo, Subpath, Public, Secret)
                        end;
                    %% No overlapping paths were found; just insert the new entry
                    _ ->
                        p_write_row(KeyId, Repo, Subpath, Public, Secret)
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_get_lease(Public) -> lease_get_result()
                                 when Public :: binary().
p_get_lease(Public) ->
    {ok, MaxLeaseTime} = application:get_env(cvmfs_gateway, max_lease_time),

    MS = ets:fun2ms(fun(#lease{public = P} = Lease) when P =:= Public ->
                            Lease
                    end),

    T = fun() ->
                CurrentTime = erlang:system_time(milli_seconds),

                case mnesia:select(lease, MS) of
                    [] ->
                        {error, invalid_lease};
                    [Lease | _]  ->
                        #lease{time = Time, path = Path} = Lease,
                        RemainingTime = MaxLeaseTime - (CurrentTime - Time),
                        case RemainingTime > 0 of
                            true ->
                                {ok, Lease};
                            false ->
                                mnesia:delete({lease, Path}),
                                {error, lease_expired}
                        end
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_end_lease(Public :: binary()) -> ok.
p_end_lease(Public) ->
    MS = ets:fun2ms(fun(#lease{public = Pub, path = Path}) when Pub =:= Public ->
                            Path
                    end),
    T = fun() ->
                case mnesia:select(lease, MS) of
                    [Path | _] ->
                        mnesia:delete({lease, Path});
                    [] ->
                        ok
                end
        end,
    {atomic, Result} = mnesia:sync_transaction(T),
    Result.


-spec p_get_leases() -> Leases :: [#lease{}].
p_get_leases() ->
    T = fun() ->
                mnesia:foldl(fun(Lease, Acc) -> [Lease | Acc] end, [], lease)
        end,
    {atomic, Result} = mnesia:transaction(T),
    Result.


-spec p_clear_leases() -> ok.
p_clear_leases() ->
    {atomic, Result} = mnesia:clear_table(lease),
    Result.


-spec p_write_row(KeyId, Repo, Path, Public, Secret) -> ok
                                                     when KeyId :: binary(),
                                                          Repo :: binary(),
                                                          Path :: binary(),
                                                          Public :: binary(),
                                                          Secret :: binary().
p_write_row(KeyId, Repo, Path, Public, Secret) ->
    mnesia:write(#lease{path = {Repo, Path},
                        key_id = KeyId,
                        public = Public,
                        secret = Secret,
                        time = erlang:system_time(milli_seconds)}).
