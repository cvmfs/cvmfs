%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_be public API
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_be).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/1
        ,new_lease/3, end_lease/2
        ,submit_payload/3]).

-export([get_repos/1
        ,check_hmac/4
        ,unique_id/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% Type specifications
%%%===================================================================
-type new_lease_result() :: {ok, LeaseToken :: binary()} |
                            {error, invalid_key} |
                            {error, invalid_path} |
                            {path_busy, TimeRemaining :: binary()}.
-type submission_error() :: {error,
                             lease_expired |
                             invalid_lease |
                             invalid_key |
                             invalid_macaroon}.
-type submit_payload_result() :: {ok, payload_added} |
                                 {ok, payload_added, lease_ended} |
                                 submission_error().


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
%% Requests the initiation of a new lease where KeyId will be used to
%% modify Path. Either returns error messages if user is invalid or
%% the user is not allowed to modify the path, or returns a lease token.
%%
%% The lease token is needed for making any other request.
%%
%% @end
%%--------------------------------------------------------------------
-spec new_lease(Uid, KeyId, Path) -> new_lease_result()
                                         when Uid :: binary(),
                                              KeyId :: binary(),
                                              Path :: binary().
new_lease(Uid, KeyId, Path) ->
    gen_server:call(?MODULE, {be_req, new_lease, {Uid, KeyId, Path}}).


%%--------------------------------------------------------------------
%% @doc
%% Ends the lease identified by LeaseToken.
%%
%%
%% @end
%%--------------------------------------------------------------------
-spec end_lease(Uid, LeaseToken) -> ok | {error, invalid_macaroon}
                                        when Uid ::binary(),
                                             LeaseToken :: binary().
end_lease(Uid, LeaseToken) ->
    gen_server:call(?MODULE, {be_req, end_lease, {Uid, LeaseToken}}).

%%--------------------------------------------------------------------
%% @doc
%% Submit a new payload
%%
%%
%% @end
%%--------------------------------------------------------------------
-spec submit_payload(Uid, LeaseToken, Payload) ->
                            submit_payload_result()
                                when Uid :: binary(),
                                     LeaseToken :: binary(),
                                     Payload :: binary().
submit_payload(Uid, LeaseToken, Payload) ->
    gen_server:call(?MODULE, {be_req, submit_payload, {Uid, LeaseToken,
                                                       Payload}}).


-spec get_repos(Uid :: binary()) -> [binary()].
get_repos(Uid) ->
    gen_server:call(?MODULE, {be_req, get_repos, Uid}).


-spec check_hmac(Uid, Message, KeyId, HMAC) -> boolean()
                                                   when Uid :: binary(),
                                                        Message :: binary(),
                                                        KeyId :: binary(),
                                                        HMAC :: binary().
check_hmac(Uid, Message, KeyId, HMAC) ->
    gen_server:call(?MODULE, {be_req, check_hmac, {Uid, Message, KeyId, HMAC}}).


-spec unique_id() -> binary().
unique_id() ->
    gen_server:call(?MODULE, {be_req, unique_id}).


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
init([]) ->
    process_flag(trap_exit, true),
    ok = quickrand:seed(),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({be_req, new_lease, {Uid, KeyId, Path}}, _From, State) ->
    case p_new_lease(KeyId, Path) of
        {ok, LeaseToken} ->
            lager:info("Request received: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                       [Uid, KeyId, Path, LeaseToken]),
            {reply, {ok, LeaseToken}, State};
        Other ->
            lager:info("Request received: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                       [Uid, KeyId, Path, Other]),
            {reply, Other, State}
    end;
handle_call({be_req, end_lease, {Uid, LeaseToken}}, _From, State) ->
    Reply = p_end_lease(LeaseToken),
    lager:info("Request received: Uid: ~p - {end_lease, ~p} -> Reply: ~p",
               [Uid, LeaseToken, Reply]),
    {reply, Reply, State};
handle_call({be_req, submit_payload, {Uid, LeaseToken, Payload}}, _From, State) ->
    Reply = p_submit_payload(LeaseToken, Payload),
    lager:info("Request received: Uid: ~p - {submit_payload, {~p, ~p}} -> Reply: ~p",
               [Uid, LeaseToken, <<"payload_not_shown">>, Reply]),
    {reply, Reply, State};
handle_call({be_req, get_repos, Uid}, _From, State) ->
    Reply = p_get_repos(),
    lager:info("Request received: Uid: ~p - {get_repos} -> Reply: ~p",
               [Uid, Reply]),
    {reply, Reply, State};
handle_call({be_req, check_hmac, {Uid, Message, KeyId, HMAC}}, _From, State) ->
    Reply = p_check_hmac(Message, KeyId, HMAC),
    lager:info("Request received: Uid: ~p - {check_hmac, {~p, ~p, ~p}} -> Reply: ~p",
               [Uid, Message, KeyId, HMAC, Reply]),
    {reply, Reply, State};
handle_call({be_req, unique_id}, _From, State) ->
    Reply = p_unique_id(),
    lager:info("Request received: {unique_id} -> Reply: ~p", [Reply]),
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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec p_new_lease(KeyId, Path) -> new_lease_result()
                                      when KeyId :: binary(),
                                           Path :: binary().
p_new_lease(KeyId, Path) ->
    % Check if user is registered with the cvmfs_auth service and
    % which paths he is allowed to modify
    [Repo | _]  = binary:split(Path, <<"/">>),
    case cvmfs_auth:check_keyid_for_repo(KeyId, Repo) of
        {ok, true} ->
            {Public, Secret, Token} = p_generate_token(KeyId, Path),
            case cvmfs_lease:request_lease(KeyId, Path, Public, Secret) of
                ok ->
                    {ok, Token};
                {busy, TimeRemaining} ->
                    {path_busy, TimeRemaining}
            end;
        {ok, false} ->
            {error, invalid_key};
        Error ->
            Error
    end.


-spec p_end_lease(LeaseToken) -> ok | {error, invalid_macaroon}
                                     when LeaseToken :: binary().
p_end_lease(LeaseToken) ->
    case macaroon:deserialize(LeaseToken) of
        {ok, M} ->
            Public = macaroon:identifier(M),
            cvmfs_lease:end_lease(Public);
        _ ->
            {error, invalid_macaroon}
    end.

-spec p_submit_payload(LeaseToken, Payload) ->
                              submit_payload_result()
                                  when LeaseToken :: binary(),
                                       Payload :: binary().
p_submit_payload(LeaseToken, Payload) ->
    case p_check_payload(LeaseToken, Payload) of
        {ok, _Public} ->
            %% TODO: submit payload to GW
            {ok, payload_added};
        {error, {unverified_caveat, <<"time < ", _/binary>>}} ->
            {error, lease_expired};
        {error, Reason} ->
            {error, Reason}
    end.


-spec p_generate_token(KeyId, Path) -> {Token, Public, Secret}
                                           when KeyId :: binary(),
                                                Path :: binary(),
                                                Token :: binary(),
                                                Public :: binary(),
                                                Secret :: binary().
p_generate_token(KeyId, Path) ->
    Secret = enacl_p:randombytes(macaroon:suggested_secret_length()),
    Public = <<KeyId/binary,Path/binary>>,
    Location = <<"">>, %% Location isn't used
    M = macaroon:create(Location, Secret, Public),

    {ok, MaxLeaseTime} = application:get_env(cvmfs_services, max_lease_time),
    Time = erlang:system_time(milli_seconds) + MaxLeaseTime,

    M1 = macaroon:add_first_party_caveat(M,
                                         "time < " ++ erlang:integer_to_binary(Time)),

    %%M3 = macaroon:add_first_party_caveat(M2, "path = " ++ Path),

    {ok, Token} = macaroon:serialize(M1),
    {Public, Secret, Token}.


-spec p_check_payload(LeaseToken, Payload) ->
                             {ok, Public} | {error, invalid_macaroon |
                                             {unverified_caveat, Caveat}}
                                 when LeaseToken :: binary(),
                                      Payload :: binary(),
                                      Public :: binary(),
                                      Caveat :: binary().
p_check_payload(LeaseToken, _Payload) ->
    % Here we should perform all sanity checks on the request
    % Verify lease token (check user, check time-stamp, extract path).
    case  macaroon:deserialize(LeaseToken) of
        {ok, M} ->
            Public = macaroon:identifier(M),
            case cvmfs_lease:check_lease(Public) of
                {ok, Secret} ->
                    CheckTime = fun(<<"time < ", Exp/binary>>) ->
                                        erlang:binary_to_integer(Exp)
                                            > erlang:system_time(milli_seconds);
                                   (_) ->
                                        false
                                end,
                    %% CheckPaths = fun(_) ->
                    %%                      true
                    %%              end,

                    V = macaroon_verifier:create(),
                    V1 = macaroon_verifier:satisfy_general(V, CheckTime),
                    %% V3 = macaroon_verifier:satisfy_general(V2, CheckPaths),

                    case macaroon_verifier:verify(V1, M, Secret) of
                        ok ->
                            {ok, Public};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, invalid_macaroon}
    end.


-spec p_get_repos() -> [binary()].
p_get_repos() ->
    cvmfs_auth:get_repos().


-spec p_check_hmac(Message, KeyId, HMAC) -> boolean()
                                                when Message :: binary(),
                                                     KeyId :: binary(),
                                                     HMAC :: binary().
p_check_hmac(Message, KeyId, HMAC) ->
    cvmfs_auth:check_hmac(Message, KeyId, HMAC).


-spec p_unique_id() -> binary().
p_unique_id() ->
    uuid:get_v4_urandom().
