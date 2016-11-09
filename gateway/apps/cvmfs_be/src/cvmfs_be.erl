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
        ,new_lease/2, end_lease/1
        ,submit_payload/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% Type specifications
%%%===================================================================
-type new_lease_result() :: {ok, LeaseToken :: binary()} |
                            {error, invalid_user} |
                            {error, invalid_path} |
                            {busy, TimeRemaining :: binary()}.
-type submission_error() :: {error,
                             lease_expired |
                             invalid_lease |
                             invalid_user |
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
%% Requests the initiation of a new lease where User will modify
%% Path. Either returns error messages if user is invalid or the user
%% is not allowed to modify the path, or returns a lease token.
%%
%% The lease token is needed for making any other request.
%%
%% @end
%%--------------------------------------------------------------------
-spec new_lease(User, Path) -> new_lease_result()
                                   when User :: binary(),
                                        Path :: binary().
new_lease(User, Path) ->
    gen_server:call(?MODULE, {be_req, new_lease, {User, Path}}).


%%--------------------------------------------------------------------
%% @doc
%% Ends the lease identified by LeaseToken.
%%
%%
%% @end
%%--------------------------------------------------------------------
-spec end_lease(LeaseToken) -> ok | {error, invalid_macaroon}
                                   when LeaseToken :: binary().
end_lease(LeaseToken) ->
    gen_server:call(?MODULE, {be_req, end_lease, LeaseToken}).

%%--------------------------------------------------------------------
%% @doc
%% Submit a new payload
%%
%%
%% @end
%%--------------------------------------------------------------------
-spec submit_payload(User, LeaseToken, Payload, Final) ->
                            submit_payload_result()
                                when User :: binary(),
                                     LeaseToken :: binary(),
                                     Payload :: binary(),
                                     Final :: boolean().
submit_payload(User, LeaseToken, Payload, Final) ->
    gen_server:call(?MODULE, {be_req, submit_payload, {User,
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
%% @end
%%--------------------------------------------------------------------
handle_call({be_req, new_lease, {User, Path}}, _From, State) ->
    case p_new_lease(User, Path) of
        {ok, LeaseToken} ->
            lager:info("Request received: {new_lease, {~p, ~p}} -> Reply: ~p",
                       [User, Path, LeaseToken]),
            {reply, {ok, LeaseToken}, State};
        Other ->
            lager:info("Request received: {new_lease, {~p, ~p}} -> Reply: ~p",
                       [User, Path, Other]),
            {reply, Other, State}
    end;
handle_call({be_req, end_lease, LeaseToken}, _From, State) ->
    Reply = p_end_lease(LeaseToken),
    lager:info("Request received: {end_lease, ~p} -> Reply: ~p", [LeaseToken, Reply]),
    {reply, Reply, State};
handle_call({be_req, submit_payload, {User, LeaseToken, Payload, Final}}, _From, State) ->
    Reply = p_submit_payload(User, LeaseToken, Payload, Final),
    lager:info("Request received: {submit_payload, {~p, ~p, ~p, ~p}} -> Reply: ~p",
               [User, LeaseToken, Payload, Final, Reply]),
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

-spec p_new_lease(User, Path) -> new_lease_result()
                                     when User :: binary(),
                                          Path :: binary().
p_new_lease(User, Path) ->
    % Check if user is registered with the cvmfs_auth service and
    % which paths he is allowed to modify
    case cvmfs_auth:get_user_permissions(User) of
        {ok, Paths} ->
            case lists:member(Path, Paths) of
                false ->
                    {error, invalid_path};
                true ->
                    {Public, Secret, Token} = p_generate_token(User, Path),
                    case cvmfs_lease:request_lease(User, Path, Public, Secret) of
                        ok ->
                            {ok, Token};
                        {busy, TimeRemaining} ->
                            {error, path_busy, TimeRemaining}
                    end
            end;
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

-spec p_submit_payload(User, LeaseToken, Payload, Final) ->
                              submit_payload_result()
                                  when User :: binary(),
                                       LeaseToken :: binary(),
                                       Payload :: binary(),
                                       Final :: boolean().
p_submit_payload(User, LeaseToken, Payload, Final) ->
    case p_check_payload(User, LeaseToken, Payload) of
        {ok, Public} ->
            %% TODO: submit payload to GW

            case Final of
                true ->
                    cvmfs_lease:end_lease(Public),
                    {ok, payload_added, lease_ended};
                false ->
                    {ok, payload_added}
            end;
        {error, {unverified_caveat, <<"time < ", _/binary>>}} ->
            {error, lease_expired};
        {error, {unverified_caveat, <<"user = ", _/binary>>}} ->
            {error, invalid_user};
        {error, Reason} ->
            {error, Reason}
    end.


-spec p_generate_token(User, Path) -> {Token, Public, Secret}
                                          when User :: binary(),
                                               Path :: binary(),
                                               Token :: binary(),
                                               Public :: binary(),
                                               Secret :: binary().
p_generate_token(User, Path) ->
    Secret = enacl_p:randombytes(macaroon:suggested_secret_length()),
    Public = <<User/binary,Path/binary>>,
    Location = <<"">>, %% Location isn't used
    M = macaroon:create(Location, Secret, Public),
    M1 = macaroon:add_first_party_caveat(M, "user = " ++ User),

    {ok, MaxLeaseTime} = application:get_env(cvmfs_lease, max_lease_time),
    Time = erlang:system_time(milli_seconds) + MaxLeaseTime,

    M2 = macaroon:add_first_party_caveat(M1,
                                         "time < " ++ erlang:integer_to_binary(Time)),

    %%M3 = macaroon:add_first_party_caveat(M2, "path = " ++ Path),

    {ok, Token} = macaroon:serialize(M2),
    {Public, Secret, Token}.


-spec p_check_payload(User, LeaseToken, Payload) ->
                             {ok, Public} | {error, invalid_macaroon |
                                             {unverified_caveat, Caveat}}
                                 when User :: binary(),
                                      LeaseToken :: binary(),
                                      Payload :: binary(),
                                      Public :: binary(),
                                      Caveat :: binary().
p_check_payload(User, LeaseToken, _Payload) ->
    % Here we should perform all sanity checks on the request
    % Verify lease token (check user, check time-stamp, extract path).
    case  macaroon:deserialize(LeaseToken) of
        {ok, M} ->
            Public = macaroon:identifier(M),
            case cvmfs_lease:check_lease(Public) of
                {ok, Secret} ->
                    CheckUser = fun(<<"user = ", U/binary>>) ->
                                        U =:= User;
                                   (_) ->
                                        false
                                end,
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
                    V1 = macaroon_verifier:satisfy_general(V, CheckUser),
                    V2 = macaroon_verifier:satisfy_general(V1, CheckTime),
                    %% V3 = macaroon_verifier:satisfy_general(V2, CheckPaths),

                    case macaroon_verifier:verify(V2, M, Secret) of
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
