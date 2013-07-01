#include <gtest/gtest.h>

#include "testutil.h"

// This is very meta!
// We are testing the utility functions used for testing...

TEST(T_TestUtilities, GetParentPid) {
  const pid_t my_pid        = getpid();
  const pid_t my_parent_pid = getppid();

  ASSERT_GT (my_pid, 1);
  ASSERT_GT (my_parent_pid, 0);

  const pid_t result = GetParentPid(my_pid);
  EXPECT_EQ (my_parent_pid, result);
}
