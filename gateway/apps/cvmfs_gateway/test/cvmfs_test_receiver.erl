%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc cvmfs_test_receiver
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_test_receiver).

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%%%===================================================================
%%% Type specifications
%%%===================================================================
-type submission_error() :: {error,
                             lease_expired |
                             invalid_lease |
                             invalid_key |
                             invalid_macaroon}.
-type submit_payload_result() :: {ok, payload_added} |
                                 {ok, payload_added, lease_ended} |
                                 submission_error().
-type payload_submission_data() :: {LeaseToken :: binary()
                                   ,Payload :: binary()
                                   ,Digest :: binary()
                                   ,HeaderSize :: integer()}.


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
    gen_server:start_link(?MODULE, Args, []).


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
init(_Args) ->
    process_flag(trap_exit, true),
    lager:info("CVMFS Test Receiver initialized at PID ~p.", [self()]),

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
handle_call({worker_req, generate_token, KeyId, Path, MaxLeaseTime}, _From, State) ->
    Reply = p_generate_token(KeyId, Path, MaxLeaseTime),
    lager:debug("Worker ~p request: {generate_token, {~p, ~p, ~p}} -> Reply: ~p",
                [self(), KeyId, Path, MaxLeaseTime, Reply]),
    {reply, Reply, State};
handle_call({worker_req, get_token_id, Token}, _From, State) ->
    Reply = p_get_token_id(Token),
    lager:debug("Worker ~p request: {get_token_id, ~p} -> Reply: ~p",
                [self(), Token, Reply]),
    {reply, Reply, State};
handle_call({worker_req, submit_payload, SubmissionData, Secret}, _From, State) ->
    Reply = p_submit_payload(SubmissionData, Secret),
    lager:debug("Worker ~p request: {submit_payload, {~p, ~p}} -> Reply: ~p",
                [self(), SubmissionData, Secret, Reply]),
    {reply, Reply, State};
handle_call({worker_req, commit, LeasePath, OldRootHash, NewRootHash, RepoTag}, _From, State) ->
    Reply = p_commit(LeasePath, OldRootHash, NewRootHash, RepoTag),
    lager:debug("Worker ~p request: {commit, ~p, ~p, ~p, ~p, ~p} -> Reply: ~p",
                [self(), LeasePath, OldRootHash, NewRootHash, RepoTag, Reply]),
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
handle_info({Port, {exit_status, Status}}, State) ->
    lager:info("Worker process ~p exited with status: ~p", [Port, Status]),
    {noreply, State};
handle_info({'EXIT', Port, Reason}, State) ->
    lager:info("Port ~p exited with reason: ~p", [Port, Reason]),
    {noreply, State};
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
terminate(Reason, State) ->
    #{worker := WorkerPort} = State,
    port_close(WorkerPort),
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

-spec p_generate_token(KeyId, Path, MaxLeaseTime) -> {Token, Public, Secret}
                                                         when KeyId :: binary(),
                                                              Path :: binary(),
                                                              MaxLeaseTime :: integer(),
                                                              Token :: binary(),
                                                              Public :: binary(),
                                                              Secret :: binary().
p_generate_token(KeyId, Path, MaxLeaseTime) ->
    Secret = enacl_p:randombytes(macaroon:suggested_secret_length()),
    Public = <<KeyId/binary,Path/binary>>,
    Location = <<"">>, %% Location isn't used
    M = macaroon:create(Location, Secret, Public),

    Time = erlang:system_time(milli_seconds) + MaxLeaseTime,

    M1 = macaroon:add_first_party_caveat(M,
                                         "time < " ++ erlang:integer_to_binary(Time)),

    M2 = macaroon:add_first_party_caveat(M1, "path = " ++ Path),

    {ok, Token} = macaroon:serialize(M2),
    {Public, Secret, Token}.


-spec p_get_token_id(Token) -> {ok, PublicId} | {error, invalid_macaroon}
                                   when Token :: binary(),
                                        PublicId :: binary().
p_get_token_id(Token) ->
    case macaroon:deserialize(Token) of
        {ok, M} ->
            {ok, macaroon:identifier(M)};
        _ ->
            {error, invalid_macaroon}
    end.


-spec p_submit_payload(SubmissionData, Secret) -> submit_payload_result()
                                                    when SubmissionData :: payload_submission_data(),
                                                         Secret :: binary().
p_submit_payload({LeaseToken, _Payload, _Digest, _HeaderSize}, Secret) ->
    CheckTime = fun(<<"time < ", Exp/binary>>) ->
                        erlang:binary_to_integer(Exp)
                            > erlang:system_time(milli_seconds);
                   (_) ->
                        false
                end,
    CheckPaths = fun(<<"path = ", _Path/binary>>) ->
                         true;
                    (_) ->
                         false
                 end,

    V = macaroon_verifier:create(),
    V1 = macaroon_verifier:satisfy_general(V, CheckTime),
    V2 = macaroon_verifier:satisfy_general(V1, CheckPaths),

    {ok, M} = macaroon:deserialize(LeaseToken),
    case macaroon_verifier:verify(V2, M, Secret) of
        ok ->
            %% TODO: submit payload to GW
            {ok, payload_added};
        {error, {unverified_caveat, <<"time < ", _/binary>>}} ->
            {error, lease_expired};
        {error, Reason} ->
            {error, Reason}
    end.


-spec p_commit(LeasePath :: binary(),
               OldRootHash :: binary(),
               NewRootHash :: binary(),
               RepoTag :: cvmfs_common_types:repository_tag())
              -> ok | {error, merge_error | io_error | worker_timeout}.
p_commit(_Path, _OldRootHash, _NewRootHash, _RepoTag) ->
    timer:sleep(100),
    ok.

