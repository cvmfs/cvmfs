/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "clientctx.h"
#include "interrupt.h"

namespace {
class TestInterruptCue : public InterruptCue {
 public:
  virtual bool IsCanceled() { return true; }
};
}  // anonymous namespace

TEST(T_ClientCtx, GetInstance) {
  // Noop, don't crash
  ClientCtx::CleanupInstance();

  TestInterruptCue tic;
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Set(1, 2, 3, &tic);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::CleanupInstance();
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::CleanupInstance();
}

TEST(T_ClientCtx, GetSet) {
  uid_t uid;
  gid_t gid;
  pid_t pid;
  InterruptCue *ic;
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  EXPECT_EQ(uid_t(-1), uid);
  EXPECT_EQ(gid_t(-1), gid);
  EXPECT_EQ(pid_t(-1), pid);
  EXPECT_EQ(NULL, ic);

  TestInterruptCue tic;
  ClientCtx::GetInstance()->Set(1, 2, 3, &tic);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_EQ(1U, uid);
  EXPECT_EQ(2U, gid);
  EXPECT_EQ(3, pid);
  EXPECT_TRUE(ic->IsCanceled());

  ClientCtx::GetInstance()->Set(5, 6, 7, &tic);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_EQ(5U, uid);
  EXPECT_EQ(6U, gid);
  EXPECT_EQ(7, pid);
  EXPECT_TRUE(ic->IsCanceled());

  ClientCtx::GetInstance()->Unset();
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_EQ(uid_t(-1), uid);
  EXPECT_EQ(gid_t(-1), gid);
  EXPECT_EQ(pid_t(-1), pid);
  EXPECT_EQ(NULL, ic);

  ClientCtx::GetInstance()->Set(10, 11, 12, &tic);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_EQ(10U, uid);
  EXPECT_EQ(11U, gid);
  EXPECT_EQ(12, pid);
  EXPECT_TRUE(ic->IsCanceled());

  ClientCtx::CleanupInstance();
}

TEST(T_ClientCtx, Guard) {
  uid_t uid;
  gid_t gid;
  pid_t pid;
  InterruptCue *ic;

  {
    TestInterruptCue tic;
    ClientCtxGuard guard(1, 2, 3, &tic);
    EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
    ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
    EXPECT_EQ(1U, uid);
    EXPECT_EQ(2U, gid);
    EXPECT_EQ(3, pid);
    EXPECT_TRUE(ic->IsCanceled());
  }
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());

  InterruptCue default_ic;
  ClientCtx::GetInstance()->Set(4, 5, 6, &default_ic);
  {
    TestInterruptCue tic;
    ClientCtxGuard guard(7, 8, 9, &tic);
    EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
    ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
    EXPECT_EQ(7U, uid);
    EXPECT_EQ(8U, gid);
    EXPECT_EQ(9, pid);
    EXPECT_TRUE(ic->IsCanceled());
  }
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid, &ic);
  EXPECT_EQ(4U, uid);
  EXPECT_EQ(5U, gid);
  EXPECT_EQ(6, pid);
  EXPECT_FALSE(ic->IsCanceled());

  ClientCtx::CleanupInstance();
}
