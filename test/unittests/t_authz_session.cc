/**
 * This file is part of the CernVM File System.
 */

#include <alloca.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include "authz/authz.h"
#include "authz/authz_fetch.h"
#include "authz/authz_session.h"
#include "statistics.h"
#include "util/platform.h"
#include "util/pointer.h"


class TestAuthzFetcher : public AuthzFetcher {
 public:
  virtual AuthzStatus Fetch(const QueryInfo &query_info,
                            AuthzToken *authz_token,
                            unsigned *ttl) {
    *ttl = next_ttl;
    *authz_token = next_token;
    return next_status;
  }

  unsigned next_ttl;
  AuthzToken next_token;
  AuthzStatus next_status;
};


class T_AuthzSession : public ::testing::Test {
 protected:
  virtual void SetUp() {
    authz_session_mgr_ = AuthzSessionManager::Create(&authz_fetcher_,
                                                     &statistics_);
  }

  virtual void TearDown() { delete authz_session_mgr_; }

  AuthzSessionManager *authz_session_mgr_;
  TestAuthzFetcher authz_fetcher_;
  perf::Statistics statistics_;
};


TEST_F(T_AuthzSession, GetPidInfo) {
  AuthzSessionManager::PidKey pid_key;
  EXPECT_FALSE(authz_session_mgr_->GetPidInfo(-1, &pid_key));

  EXPECT_TRUE(authz_session_mgr_->GetPidInfo(getpid(), &pid_key));
  EXPECT_EQ(getpid(), pid_key.pid);
  EXPECT_EQ(getuid(), pid_key.uid);
  EXPECT_EQ(getgid(), pid_key.gid);
  EXPECT_EQ(getsid(0), pid_key.sid);
  EXPECT_EQ(0U, pid_key.deadline);

  AuthzSessionManager::PidKey init_pid_key;
  EXPECT_TRUE(authz_session_mgr_->GetPidInfo(1, &init_pid_key));
  EXPECT_EQ(1, init_pid_key.pid);
  // On docker, the following two lines are not necessarily true
  // EXPECT_EQ(0U, init_pid_key.uid);
  // EXPECT_EQ(0U, init_pid_key.gid);
  EXPECT_EQ(1, init_pid_key.sid);
  EXPECT_EQ(0U, init_pid_key.deadline);

  EXPECT_GE(pid_key.pid_bday, init_pid_key.pid_bday);
}


TEST_F(T_AuthzSession, LookupSessionKey) {
  uint64_t now = platform_monotonic_time();
  EXPECT_EQ(0U, authz_session_mgr_->pid2session_.size());
  AuthzSessionManager::SessionKey session_key;
  AuthzSessionManager::PidKey pid_key;
  EXPECT_FALSE(
      authz_session_mgr_->LookupSessionKey(-1, &pid_key, &session_key));

  EXPECT_TRUE(
      authz_session_mgr_->LookupSessionKey(getpid(), &pid_key, &session_key));
  EXPECT_EQ(getpid(), pid_key.pid);
  EXPECT_EQ(getsid(0), pid_key.sid);
  EXPECT_EQ(getuid(), pid_key.uid);
  EXPECT_EQ(getgid(), pid_key.gid);
  EXPECT_EQ(getsid(0), session_key.sid);
  EXPECT_EQ(1U, authz_session_mgr_->pid2session_.size());

  authz_session_mgr_->SweepPids(now);
  EXPECT_EQ(1U, authz_session_mgr_->pid2session_.size());
  authz_session_mgr_->SweepPids(now + AuthzSessionManager::kPidLifetime + 1);
  EXPECT_EQ(0U, authz_session_mgr_->pid2session_.size());
}


TEST_F(T_AuthzSession, LookupAuthzData) {
  AuthzSessionManager::SessionKey session_key;
  AuthzSessionManager::PidKey pid_key;
  AuthzData authz_data;

  session_key.sid = 0;
  pid_key.pid = 0;
  authz_fetcher_.next_status = kAuthzOk;
  authz_fetcher_.next_ttl = 1000;
  EXPECT_TRUE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "A", &authz_data));
  EXPECT_EQ("A", authz_data.membership);
  EXPECT_EQ(kAuthzOk, authz_data.status);
  EXPECT_LE(platform_monotonic_time() + authz_fetcher_.next_ttl,
            authz_data.deadline);

  authz_fetcher_.next_status = kAuthzNotFound;
  // Cached
  EXPECT_TRUE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "A", &authz_data));
  // Membership changed
  EXPECT_FALSE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "B", &authz_data));
  session_key.sid = 1;
  pid_key.pid = 1;
  EXPECT_FALSE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "A", &authz_data));

  authz_fetcher_.next_status = kAuthzOk;
  // Negative cache
  EXPECT_FALSE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "A", &authz_data));
  authz_session_mgr_->SweepCreds(platform_monotonic_time()
                                 + authz_fetcher_.next_ttl);
  EXPECT_TRUE(authz_session_mgr_->LookupAuthzData(
      pid_key, session_key, "A", &authz_data));
}


TEST_F(T_AuthzSession, IsMemberOf) {
  authz_fetcher_.next_status = kAuthzOk;
  authz_fetcher_.next_ttl = 1000;
  EXPECT_TRUE(authz_session_mgr_->IsMemberOf(1, "A"));
  EXPECT_FALSE(authz_session_mgr_->IsMemberOf(1, "B"));
  EXPECT_FALSE(authz_session_mgr_->IsMemberOf(-1, "A"));
}


TEST_F(T_AuthzSession, ClearSessionCache) {
  EXPECT_EQ(0, statistics_.Lookup("authz.no_session")->Get());
  authz_fetcher_.next_status = kAuthzOk;
  authz_fetcher_.next_ttl = 1000;
  EXPECT_TRUE(authz_session_mgr_->IsMemberOf(1, "A"));
  EXPECT_FALSE(authz_session_mgr_->IsMemberOf(1, "B"));
  EXPECT_EQ(1, statistics_.Lookup("authz.no_session")->Get());

  authz_session_mgr_->ClearSessionCache();
  EXPECT_EQ(0, statistics_.Lookup("authz.no_session")->Get());

  EXPECT_TRUE(authz_session_mgr_->IsMemberOf(1, "B"));
  EXPECT_FALSE(authz_session_mgr_->IsMemberOf(1, "A"));
  EXPECT_EQ(1, statistics_.Lookup("authz.no_session")->Get());
}


TEST_F(T_AuthzSession, GetTokenCopy) {
  authz_fetcher_.next_status = kAuthzOk;
  authz_fetcher_.next_ttl = 0;
  AuthzToken fetched_token;
  fetched_token.type = kTokenX509;
  fetched_token.data = smalloc(1);
  reinterpret_cast<char *>(fetched_token.data)[0] = 'X';
  fetched_token.size = 1;
  authz_fetcher_.next_token = fetched_token;

  UniquePtr<AuthzToken> tokenX(authz_session_mgr_->GetTokenCopy(1, "A"));
  ASSERT_TRUE(tokenX.IsValid());
  EXPECT_EQ(kTokenX509, tokenX->type);
  EXPECT_EQ(1U, tokenX->size);
  EXPECT_EQ('X', reinterpret_cast<char *>(tokenX->data)[0]);
  free(tokenX->data);

  reinterpret_cast<char *>(fetched_token.data)[0] = 'Y';
  UniquePtr<AuthzToken> tokenY(authz_session_mgr_->GetTokenCopy(1, "A"));
  ASSERT_TRUE(tokenY.IsValid());
  EXPECT_EQ(kTokenX509, tokenY->type);
  EXPECT_EQ(1U, tokenY->size);
  EXPECT_EQ('Y', reinterpret_cast<char *>(tokenY->data)[0]);
  free(tokenY->data);

  EXPECT_EQ(NULL, authz_session_mgr_->GetTokenCopy(-1, "A"));
}
