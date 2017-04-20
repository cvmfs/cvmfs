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
        ,new_lease/3, end_lease/3
        ,submit_payload/2]).

-export([get_repos/1
        ,check_hmac/4
        ,unique_id/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% Constants
% Timeout of asychronous request handling == 30min
-define(ASYNC_TIMEOUT, 1000 * 60 * 30).


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
-spec end_lease(Uid, LeaseToken, Commit) -> ok | {error, invalid_macaroon}
                                        when Uid :: binary(),
                                             LeaseToken :: binary(),
                                             Commit :: boolean().
end_lease(Uid, LeaseToken, Commit) ->
    gen_server:call(?MODULE, {be_req, end_lease, {Uid, LeaseToken, Commit}}).


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
%% Note on asynchronicity:
%%
%% As part of each request, the frontend (cvmfs_fe) calls into the
%% backend (cvmfs_be), which then calls into different submodules.
%% Each backend API call will be made from different processes,
%% since Cowboy spawns a new process for each request. To avoid
%% blocking the gen_server, in the handle_call callback, we always
%% return {noreply, State}, to block the calling process and
%% dispatch the actual request handling to a new subprocess, which
%% will later complete the request using gen_server:reply.
%%
%% @end
%%--------------------------------------------------------------------
handle_call({be_req, new_lease, {Uid, KeyId, Path}}, From, State) ->
    Task = fun() ->
                   case p_new_lease(KeyId, Path) of
                       {ok, LeaseToken} ->
                           lager:info("Backend request: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                                      [Uid, KeyId, Path, LeaseToken]),
                           gen_server:reply(From, {ok, LeaseToken});
                       Other ->
                           lager:info("Backend request: Uid: ~p - {new_lease, {~p, ~p}} -> Reply: ~p",
                                      [Uid, KeyId, Path, Other]),
                           gen_server:reply(From, Other)
                   end
           end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT};
handle_call({be_req, end_lease, {Uid, LeaseToken, Commit}}, From, State) ->
    Task = fun() ->
                   Reply = p_end_lease(LeaseToken, Commit),
                   lager:info("Backend request: Uid: ~p - {end_lease, ~p, ~p} -> Reply: ~p",
                              [Uid, LeaseToken, Commit, Reply]),
                   gen_server:reply(From, Reply)
           end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT};
handle_call({be_req, submit_payload, {Uid, SubmissionData}}, From, State) ->
    Task = fun() ->
                   Reply = p_submit_payload(SubmissionData),
                   {LeaseToken, _Payload, Digest, HeaderSize} = SubmissionData,
                   lager:info("Backend request: Uid: ~p - {submit_payload, {~p, ~p, ~p, ~p}} -> Reply: ~p",
                              [Uid, LeaseToken, <<"payload_not_shown">>, Digest, HeaderSize, Reply]),
                   gen_server:reply(From, Reply)
           end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT};
handle_call({be_req, get_repos, Uid}, From, State) ->
    Task = fun() ->
                   Reply = p_get_repos(),
                   lager:info("Backend request: Uid: ~p - {get_repos} -> Reply: ~p",
                              [Uid, Reply]),
                   gen_server:reply(From, Reply)
              end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT};
handle_call({be_req, check_hmac, {Uid, Message, KeyId, HMAC}}, From, State) ->
    Task = fun() ->
                   Reply = p_check_hmac(Message, KeyId, HMAC),
                   lager:info("Backend request: Uid: ~p - {check_hmac, {~p, ~p, ~p}} -> Reply: ~p",
                              [Uid, Message, KeyId, HMAC, Reply]),
                   gen_server:reply(From, Reply)
           end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT};
handle_call({be_req, unique_id}, From, State) ->
    Task = fun() ->
                   Reply = p_unique_id(),
                   lager:info("Backend request: {unique_id} -> Reply: ~p", [Reply]),
                   gen_server:reply(From,Reply)
           end,
    spawn_link(Task),
    {noreply, State, ?ASYNC_TIMEOUT}.


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
handle_info({'EXIT', Pid, normal}, State) ->
    lager:info("Task ~p finished", [Pid]),
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


-spec p_end_lease(LeaseToken, Commit) -> ok | {error, invalid_macaroon}
                                             when LeaseToken :: binary(),
                                                  Commit :: boolean().
p_end_lease(LeaseToken, _Commit) ->
    Result = case cvmfs_receiver:get_token_id(LeaseToken) of
                 {ok, Public} ->
                     %% TODO: If Commit is true, the commit operation needs to be triggered here,
                     %%       before ending the active lease
                     cvmfs_lease:end_lease(Public);
                 _ ->
                     {error, invalid_macaroon}
             end,
    Result.


-spec p_submit_payload(SubmissionData) -> cvmfs_receiver:submit_payload_result()
                                  when SubmissionData :: cvmfs_receiver:payload_submission_data().
p_submit_payload({LeaseToken, _Payload, _Digest, _HeaderSize} = SubmissionData) ->
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
