/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <signal.h>

bool g_suppress_conditional_assert;

#include "util/exception.h"

#include "wpad.h"

using namespace std;  // NOLINT


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
  signal(SIGABRT, handler);

  g_suppress_conditional_assert = true;

  asserted = false;
  conditional_assert(0);
  int has_asserted = asserted;
  EXPECT_EQ(0, has_asserted);

  asserted = false;
  has_asserted = asserted;
  conditional_assert(1);
  EXPECT_EQ(0, has_asserted);

  signal(SIGABRT, SIG_DFL);
}

TEST_F(T_Conditional_Assert, WithAssert) {
  signal(SIGABRT, handler);

  g_suppress_conditional_assert = false;
  asserted = false;
  conditional_assert(0);
  int has_asserted = asserted;
  EXPECT_EQ(0, has_asserted);

  asserted = false;
  has_asserted = asserted;
  conditional_assert(1);
  EXPECT_EQ(1, has_asserted);

  signal(SIGABRT, SIG_DFL);
}
