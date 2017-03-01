/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, Dummy) {
  upload::SessionContext ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file"));
}
