/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <signal.h>

#include "util/exception.h"

#include "wpad.h"

using namespace std;  // NOLINT

bool g_conditional_assert;

bool asserted = false;

void handler(int sig) {
  if (sig == SIGABRT) {
    asserted = true;
  }
}

#undef assert

void assert(bool t) {
  if (!t) { raise(SIGABRT); }
}

class T_Conditional_Assert : public ::testing::Test {
 protected:
  virtual void SetUp() {
    asserted = false;
    signal(SIGABRT, handler);
  }

  virtual void TearDown() {
    signal(SIGABRT, SIG_DFL);
  }
};


TEST_F(T_Conditional_Assert, WithoutAssert) {
  g_conditional_assert = false;
  asserted = false;
  bool ret = conditional_assert(0);

  EXPECT_EQ(0, ret);
  int has_asserted = asserted;
  EXPECT_EQ(1, has_asserted);

  asserted = false;
  ret = conditional_assert(1);
  EXPECT_EQ(1, ret);
  has_asserted = asserted;
  EXPECT_EQ(0, has_asserted);
}

TEST_F(T_Conditional_Assert, WithAssert) {
  signal(SIGABRT, handler);

  g_conditional_assert = true;
  asserted = false;
  bool ret = conditional_assert(0);
  EXPECT_EQ(0, ret);
  int has_asserted = asserted;
  EXPECT_EQ(0, has_asserted);

  asserted = false;
  ret = conditional_assert(1);
  EXPECT_EQ(1,  ret);
  has_asserted = asserted;
  EXPECT_EQ(0, has_asserted);

  signal(SIGABRT, SIG_DFL);
}
