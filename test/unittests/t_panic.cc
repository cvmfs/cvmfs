/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"

TEST(T_Panic, Call) {
  EXPECT_THROW(PANIC(kLogStderr, "unit test"), std::runtime_error);
  EXPECT_THROW(PANIC(NULL), std::runtime_error);
}
