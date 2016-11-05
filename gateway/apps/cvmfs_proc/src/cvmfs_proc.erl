%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_proc public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_proc).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/1, stop/0
        ,new_lease/2, end_lease/1
        ,submit_payload/4]).

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
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(_) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%%--------------------------------------------------------------------
%% @doc
%% Requests the initiation of a new lease where User will modify
%% Path. Either returns error messages if user is invalid or the user
%% is not allowed to modify the path, or returns a lease token.
%%
%% The lease token is needed for making any other request.
%%
%% @spec new_lease(User, Path) -> {ok, LeaseToken} |
%%                                  {error, invalid_user} |
%%                                  {error, invalid_path}.
%% @end
%%--------------------------------------------------------------------
-spec new_lease(User, Path) -> {ok, LeaseToken} |
                                 {error, invalid_user} |
                                 {error, invalid_path}
                                     when User :: binary(),
                                          Path :: binary(),
                                          LeaseToken :: binary().
new_lease(User, Path) ->
    gen_server:call(?MODULE, {proc_req, new_lease, {User, Path}}).


%%--------------------------------------------------------------------
%% @doc
%% Ends the lease identified by LeaseToken.
%%
%%
%% @spec end_lease() -> ok | {error, invalid_token}.
%% @end
%%--------------------------------------------------------------------
-spec end_lease(LeaseToken) -> ok | {error, invalid_token}
                                       when LeaseToken :: binary().
end_lease(LeaseToken) ->
    gen_server:call(?MODULE, {proc_req, end_lease, LeaseToken}).

%%--------------------------------------------------------------------
%% @doc
%% Submit a new payload
%%
%%
%% @spec submit_payload(User, LeaseToken, Payload, Final)
%%           -> {ok, payload_added} | {ok, payload_added, lease_ended} |
%%              {error, Reason}
%% @end
%%--------------------------------------------------------------------
-spec submit_payload(User, LeaseToken, Payload, Final) ->
                            {ok, payload_added} |
                            {ok, payload_added, lease_ended} |
                            {error, Reason}
                                when User :: binary(),
                                     LeaseToken :: binary(),
                                     Payload :: binary(),
                                     Final :: boolean(),
                                     Reason :: binary().
submit_payload(User, LeaseToken, Payload, Final) ->
    gen_server:call(?MODULE, {proc_req, submit_payload, {User,
                                                         LeaseToken,
                                                         Payload,
                                                         Final}}).

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
    process_flag(trap_exit, true),
    {ok, #{}}.

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
handle_call({proc_req, new_lease, {User, Path}}, _From, State) ->
    case priv_new_lease(User, Path) of
        {ok, {LeaseToken, Public, Secret}} ->
            NewState = State#{Public => Secret},
            lager:info("Request received: {new_lease, {~p, ~p}} -> Reply: ~p", [User, Path, LeaseToken]),
            {reply, {ok, LeaseToken}, NewState};
        Other ->
            lager:info("Request received: {new_lease, {~p, ~p}} -> Reply: ~p", [User, Path, Other]),
            {reply, Other, State}
    end;
handle_call({proc_req, end_lease, LeaseToken}, _From, State) ->
    Reply = priv_end_lease(LeaseToken),
    lager:info("Request received: {end_lease, ~p} -> Reply: ~p", [LeaseToken, Reply]),
    {reply, Reply, State};
handle_call({proc_req, submit_payload, {User, LeaseToken, Payload, Final}}, _From, State) ->
    {Reply, NewState} = priv_submit_payload(User, LeaseToken, Payload, State, Final),
    lager:info("Request received: {submit_payload, {~p, ~p, ~p, ~p}} -> Reply: ~p",
               [User, LeaseToken, Payload, Final, Reply]),
    {reply, Reply, NewState}.

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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec priv_new_lease(User, Path) -> {ok, Token} |
                                      {error, invalid_user} |
                                      {error, invalid_path}
                                          when User :: binary(),
                                               Path :: binary(),
                                               Token :: binary().
priv_new_lease(User, Path) ->
    % Check if user is registered with the cvmfs_auth service and which
    % paths he is allowed to modify
    case cvmfs_auth:get_user_permissions(User) of
        user_not_found ->
            {error, invalid_user};
        {ok, Paths} ->
            case lists:member(Path, Paths) of
                false ->
                    {error, invalid_path};
                true ->
                    %% TODO: acquire lease for subpath here
                    {ok, priv_generate_token(User, Path)}
            end
    end.

-spec priv_end_lease(LeaseToken) -> ok | {error, invalid_token}
                                            when LeaseToken :: binary().
priv_end_lease(_LeaseToken) ->
    ok.

-spec priv_submit_payload(User, LeaseToken, Payload, State, Final) ->
                                 {{ok, payload_added} |
                                  {ok, payload_added, lease_ended} |
                                  {error, Reason}, State}
                                     when User :: binary(),
                                          LeaseToken :: binary(),
                                          Payload :: binary(),
                                          State :: map(),
                                          Final :: boolean(),
                                          Reason :: lease_expired | invalid_user | invalid_token.
priv_submit_payload(User, LeaseToken, Payload, State, Final) ->
    case priv_check_payload(User, LeaseToken, Payload, State) of
        ok ->
            %% TODO: submit payload to GW

            case Final of
                true ->
                    {{ok, payload_added, lease_ended},
                     priv_delete_token(State, LeaseToken)};
                false ->
                    {{ok, payload_added}, State}
            end;
        {error, {unverified_caveat, <<"time < ", _/binary>>}} ->
            {{error, lease_expired}, State};
        {error, {unverified_caveat, <<"user = ", _/binary>>}} ->
            {{error, invalid_user}, State};
        {error, _} ->
            {{error, invalid_token}, State}
    end.

-spec priv_generate_token(User, Path) -> {Token, Public, Secret}
                                             when User :: binary(),
                                                  Path :: binary(),
                                                  Token :: binary(),
                                                  Public :: binary(),
                                                  Secret :: binary().
priv_generate_token(User, Path) ->
    Secret = enacl_p:randombytes(macaroon:suggested_secret_length()),
    Public = <<User/binary,Path/binary>>,
    Location = <<"">>, %% Location isn't used
    M = macaroon:create(Location, Secret, Public),
    M1 = macaroon:add_first_party_caveat(M, "user = " ++ User),

    {ok, MaxLeaseTime} = application:get_env(cvmfs_lease, max_lease_time),
    Time = erlang:system_time(milli_seconds) + MaxLeaseTime,

    M2 = macaroon:add_first_party_caveat(M1, "time < " ++ erlang:integer_to_binary(Time)),

    %%M3 = macaroon:add_first_party_caveat(M2, "path = " ++ Path),

    {ok, Token} = macaroon:serialize(M2),
    {Token, Public, Secret}.

-spec priv_check_payload(User, LeaseToken, Payload, State) ->
                                ok | {error, invalid_token}
                                    when User :: binary(),
                                         LeaseToken :: binary(),
                                         Payload :: binary(),
                                         State :: map().
priv_check_payload(_User, _LeaseToken, _Payload, State) when map_size(State) == 0 ->
    {error, invalid_token};
priv_check_payload(User, LeaseToken, _Payload, State) ->
    % Here we should perform all sanity checks on the request

    % Verify lease token (check user, check time-stamp, extract path).
    {ok, M} = macaroon:deserialize(LeaseToken),
    Public = macaroon:identifier(M),
    #{Public := Secret} = State,
    CheckUser = fun(<<"user = ", U/binary>>) ->
                        U =:= User;
                   (_) ->
                        false
                end,
    CheckTime = fun(<<"time < ", Exp/binary>>) ->
                        erlang:binary_to_integer(Exp) > erlang:system_time(milli_seconds);
                   (_) ->
                        false
                end,
    %% CheckPaths = fun(_) ->
    %%                      true
    %%              end,

    V = macaroon_verifier:create(),
    V1 = macaroon_verifier:satisfy_general(V, CheckUser),
    V2 = macaroon_verifier:satisfy_general(V1, CheckTime),
    %% V3 = macaroon_verifier:satisfy_general(V2, CheckPaths),

    macaroon_verifier:verify(V2, M, Secret).

priv_delete_token(State, LeaseToken) ->
    {ok, Macaroon} = macaroon:deserialize(LeaseToken),
    TokenId = macaroon:identifier(Macaroon),
    maps:remove(TokenId, State).
