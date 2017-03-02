/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

using namespace upload;
typedef SessionContext::Stats Stats;

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, BasicLifeCycle) {
  SessionContext ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file"));

  Stats stats = ctx.stats();

  EXPECT_EQ(stats.buckets_created, 0u);
  EXPECT_EQ(stats.buckets_committed, 0u);
  EXPECT_EQ(stats.objects_dispatched, 0u);
  EXPECT_EQ(stats.bytes_committed, 0u);
  EXPECT_EQ(stats.bytes_dispatched, 0u);

  EXPECT_TRUE(ctx.FinalizeSession());
}
