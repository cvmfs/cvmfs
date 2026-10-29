/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "testutil.h"

// Checks our own parent; skipped if the parent is outside our PID namespace.
TEST(T_TestUtilities, GetParentPid) {
  const pid_t my_parent_pid = getppid();
  if (my_parent_pid == 0) {
    // Parent lives outside our PID namespace (container init or exec), so 0 is
    // indistinguishable from GetParentPid's error return.
    GTEST_SKIP() << "parent outside PID namespace";
  }
  EXPECT_EQ(my_parent_pid, GetParentPid(getpid()));
}

// Runs everywhere: a forked child always has a visible, non-zero parent.
TEST(T_TestUtilities, GetParentPidOfChild) {
  const pid_t child_pid = fork();
  ASSERT_GE(child_pid, 0);
  if (child_pid == 0) {
    pause();
    _exit(0);
  }
  EXPECT_EQ(getpid(), GetParentPid(child_pid));
  kill(child_pid, SIGKILL);
  waitpid(child_pid, NULL, 0);
}

// A reaped PID no longer exists, so GetParentPid returns 0.
TEST(T_TestUtilities, GetParentPidNonexistent) {
  const pid_t child_pid = fork();
  ASSERT_GE(child_pid, 0);
  if (child_pid == 0) {
    _exit(0);
  }
  waitpid(child_pid, NULL, 0);  // reaped: pid is now free
  EXPECT_EQ(0, GetParentPid(child_pid));
}


TEST(T_TestUtilities, GetExecutablePath) {
  EXPECT_NE("", GetExecutablePath("cat"));
  EXPECT_NE("", GetExecutablePath("grep"));
  EXPECT_NE("", GetExecutablePath("ls"));
  EXPECT_EQ("", GetExecutablePath("foobar1234renem"));
  EXPECT_EQ("", GetExecutablePath("test/foo"));
  EXPECT_EQ("", GetExecutablePath("../foo/bar"));
  EXPECT_EQ("", GetExecutablePath(""));
}
