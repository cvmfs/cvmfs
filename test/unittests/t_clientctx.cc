/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/clientctx.h"

TEST(T_ClientCtx, GetInstance) {
  // Noop, don't crash
  ClientCtx::CleanupInstance();

  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Set(1, 2, 3);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::CleanupInstance();
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::CleanupInstance();
}

TEST(T_ClientCtx, GetSet) {
  uid_t uid;
  gid_t gid;
  pid_t pid;
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  EXPECT_EQ(uid_t(-1), uid);
  EXPECT_EQ(gid_t(-1), gid);
  EXPECT_EQ(pid_t(-1), pid);

  ClientCtx::GetInstance()->Set(1, 2, 3);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_EQ(1U, uid);
  EXPECT_EQ(2U, gid);
  EXPECT_EQ(3, pid);

  ClientCtx::GetInstance()->Set(5, 6, 7);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_EQ(5U, uid);
  EXPECT_EQ(6U, gid);
  EXPECT_EQ(7, pid);

  ClientCtx::GetInstance()->Unset();
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_EQ(uid_t(-1), uid);
  EXPECT_EQ(gid_t(-1), gid);
  EXPECT_EQ(pid_t(-1), pid);

  ClientCtx::GetInstance()->Set(10, 11, 12);
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_EQ(10U, uid);
  EXPECT_EQ(11U, gid);
  EXPECT_EQ(12, pid);

  ClientCtx::CleanupInstance();
}

TEST(T_ClientCtx, Guard) {
  uid_t uid;
  gid_t gid;
  pid_t pid;

  {
    ClientCtxGuard guard(1, 2, 3);
    EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
    ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
    EXPECT_EQ(1U, uid);
    EXPECT_EQ(2U, gid);
    EXPECT_EQ(3, pid);
  }
  EXPECT_FALSE(ClientCtx::GetInstance()->IsSet());

  ClientCtx::GetInstance()->Set(4, 5, 6);
  {
    ClientCtxGuard guard(7, 8, 9);
    EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
    ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
    EXPECT_EQ(7U, uid);
    EXPECT_EQ(8U, gid);
    EXPECT_EQ(9, pid);
  }
  EXPECT_TRUE(ClientCtx::GetInstance()->IsSet());
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  EXPECT_EQ(4U, uid);
  EXPECT_EQ(5U, gid);
  EXPECT_EQ(6, pid);

  ClientCtx::CleanupInstance();
}
