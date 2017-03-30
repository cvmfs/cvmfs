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
        ,submit_payload/2]).

-export([get_repos/1
        ,check_hmac/4
        ,unique_id/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%%%===================================================================
%%% Type specifications
%%%===================================================================
-type new_lease_result() :: {ok, LeaseToken :: binary()} |
                            {error, invalid_key} |
                            {error, invalid_path} |
                            {path_busy, TimeRemaining :: binary()}.


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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
-spec submit_payload(Uid, SubmissionData) ->
                            cvmfs_receiver:submit_payload_result()
                                when Uid :: binary(),
                                     SubmissionData :: cvmfs_receiver:payload_submission_data().
submit_payload(Uid, SubmissionData) ->
    gen_server:call(?MODULE, {be_req, submit_payload, {Uid, SubmissionData}}).


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
            lager:info("Backend request: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                       [Uid, KeyId, Path, LeaseToken]),
            {reply, {ok, LeaseToken}, State};
        Other ->
            lager:info("Backend request: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                       [Uid, KeyId, Path, Other]),
            {reply, Other, State}
    end;
handle_call({be_req, end_lease, {Uid, LeaseToken}}, _From, State) ->
    Reply = p_end_lease(LeaseToken),
    lager:info("Backend request: Uid: ~p - {end_lease, ~p} -> Reply: ~p",
               [Uid, LeaseToken, Reply]),
    {reply, Reply, State};
handle_call({be_req, submit_payload, {Uid, SubmissionData}}, _From, State) ->
    Reply = p_submit_payload(SubmissionData),
    {LeaseToken, _Payload, Digest, HeaderSize} = SubmissionData,
    lager:info("Backend request: Uid: ~p - {submit_payload, {~p, ~p, ~p, ~p}} -> Reply: ~p",
               [Uid, LeaseToken, <<"payload_not_shown">>, Digest, HeaderSize, Reply]),
    {reply, Reply, State};
handle_call({be_req, get_repos, Uid}, _From, State) ->
    Reply = p_get_repos(),
    lager:info("Backend request: Uid: ~p - {get_repos} -> Reply: ~p",
               [Uid, Reply]),
    {reply, Reply, State};
handle_call({be_req, check_hmac, {Uid, Message, KeyId, HMAC}}, _From, State) ->
    Reply = p_check_hmac(Message, KeyId, HMAC),
    lager:info("Backend request: Uid: ~p - {check_hmac, {~p, ~p, ~p}} -> Reply: ~p",
               [Uid, Message, KeyId, HMAC, Reply]),
    {reply, Reply, State};
handle_call({be_req, unique_id}, _From, State) ->
    Reply = p_unique_id(),
    lager:info("Backend request: {unique_id} -> Reply: ~p", [Reply]),
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
            {ok, MaxLeaseTime} = application:get_env(cvmfs_services, max_lease_time),
            {Public, Secret, Token} = cvmfs_receiver:generate_token(KeyId,
                                                                    Path,
                                                                    MaxLeaseTime),
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
    Result = case cvmfs_receiver:get_token_id(LeaseToken) of
                 {ok, Public} ->
                     cvmfs_lease:end_lease(Public);
                 _ ->
                     {error, invalid_macaroon}
             end,
    Result.


-spec p_submit_payload(SubmissionData) -> cvmfs_receiver:submit_payload_result()
                                  when SubmissionData :: cvmfs_receiver:payload_submission_data().
p_submit_payload({LeaseToken, _, _, _} = SubmissionData) ->
    Result = case cvmfs_receiver:get_token_id(LeaseToken) of
        {ok, Public} ->
            case cvmfs_lease:check_lease(Public) of
                {ok, Secret} ->
                    cvmfs_receiver:submit_payload(SubmissionData, Secret);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
             end,
    Result.


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
    base64:encode(uuid:get_v4_urandom()).
