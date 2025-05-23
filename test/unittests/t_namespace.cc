/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>

#include "util/namespace.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

class T_Namespace : public ::testing::Test {
 protected:
  virtual void SetUp() { ns_features_ = CheckNamespaceFeatures(); }

  virtual void TearDown() { }

 protected:
  int WaitForChild(pid_t pid) {
    int status;
    int retval = waitpid(pid, &status, 0);
    assert(retval >= 0);
    if (WIFEXITED(status)) {
      return WEXITSTATUS(status);
    } else {
      return -1;
    }
  }

  int ns_features_;
};


TEST_F(T_Namespace, Check) {
#ifdef __APPLE__
  EXPECT_EQ(0, CheckNamespaceFeatures());
#endif
}


TEST_F(T_Namespace, User) {
  if (!(ns_features_ & kNsFeatureUserEnabled))
    return;

  int pid = fork();
  ASSERT_GE(pid, 0);
  if (pid > 0) {
    EXPECT_EQ(0, WaitForChild(0));
    return;
  }

  uid_t uid = geteuid();
  uid_t gid = getegid();

  EXPECT_EQ(kFailNsOk, CreateUserNamespace(0, 0));
  EXPECT_EQ(uid_t(0), geteuid());
  EXPECT_EQ(uid_t(0), getuid());
  EXPECT_EQ(gid_t(0), getegid());
  EXPECT_EQ(gid_t(0), getgid());
  EXPECT_EQ(kFailNsOk, CreateUserNamespace(uid, gid));
  EXPECT_EQ(uid, geteuid());
  EXPECT_EQ(gid, getuid());
  exit(testing::Test::HasFailure());
}


TEST_F(T_Namespace, UserMount) {
  if (!(ns_features_ & kNsFeatureUserEnabled))
    return;
  if (!(ns_features_ & kNsFeatureMount))
    return;

  int pid = fork();
  ASSERT_GE(pid, 0);
  if (pid > 0) {
    EXPECT_EQ(0, WaitForChild(0));
    return;
  }

  std::string cwd = GetCurrentWorkingDirectory();
  EXPECT_EQ(kFailNsOk, CreateUserNamespace(0, 0));
  EXPECT_TRUE(CreateMountNamespace());
  EXPECT_EQ(cwd, GetCurrentWorkingDirectory());

  EXPECT_TRUE(MkdirDeep("A/foo", kPrivateDirMode));
  EXPECT_TRUE(MkdirDeep("B", kPrivateDirMode));
  EXPECT_FALSE(DirectoryExists("B/foo"));
  EXPECT_TRUE(BindMount("A", "B"));
  EXPECT_TRUE(DirectoryExists("B/foo"));

  exit(testing::Test::HasFailure());
}


TEST_F(T_Namespace, UserMountPid) {
  if (!(ns_features_ & kNsFeatureUserEnabled))
    return;
  if (!(ns_features_ & kNsFeatureMount))
    return;
  if (!(ns_features_ & kNsFeaturePid))
    return;

  pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid > 0) {
    EXPECT_EQ(0, WaitForChild(pid));
    return;
  }

  int fd_parent;
  EXPECT_EQ(kFailNsOk, CreateUserNamespace(0, 0));
  EXPECT_TRUE(CreateMountNamespace());
  EXPECT_TRUE(CreatePidNamespace(&fd_parent));

  char procpid[128];
  int len = readlink("/proc/self", procpid, 127);
  if (len < 0)
    len = 0;
  procpid[len] = '\0';
  EXPECT_EQ(StringifyInt(getpid()), std::string(procpid));
  EXPECT_EQ(pid_t(1), getpid());
  pid_t anchor_pid;
  pid_t init_pid;
  EXPECT_EQ(static_cast<int>(sizeof(pid_t)),
            SafeRead(fd_parent, &anchor_pid, sizeof(pid_t)));
  EXPECT_EQ(static_cast<int>(sizeof(pid_t)),
            SafeRead(fd_parent, &init_pid, sizeof(pid_t)));
  close(fd_parent);
  EXPECT_GT(anchor_pid, 1);
  EXPECT_GT(init_pid, 1);

  exit(testing::Test::HasFailure());
}
