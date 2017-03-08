/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

using namespace upload;

class SessionContextMocked : public SessionContext {
  virtual Future<bool>* DispatchObjectPack(ObjectPack* pack) {
    delete pack;  // Discard object pack

    // Just return a completed future saying everything is "fine" (TM)
    Future<bool>* future = new Future<bool>();
    future->Set(true);
    return future;
  }
};

typedef SessionContext::Stats Stats;

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, BasicLifeCycle) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file"));

  Stats stats = ctx.stats();

  EXPECT_EQ(stats.buckets_created, 0u);
  EXPECT_EQ(stats.buckets_committed, 0u);
  EXPECT_EQ(stats.objects_dispatched, 0u);
  EXPECT_EQ(stats.bytes_committed, 0u);
  EXPECT_EQ(stats.bytes_dispatched, 0u);

  ObjectPack::BucketHandle hd = ctx.NewBucket();
  stats = ctx.stats();
  EXPECT_EQ(stats.buckets_created, 1u);

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  stats = ctx.stats();
  EXPECT_EQ(stats.buckets_committed, 1u);
  EXPECT_EQ(stats.bytes_committed, 4096u);
  EXPECT_EQ(stats.objects_dispatched, 1u);
  EXPECT_EQ(stats.bytes_dispatched, 4096u);

  EXPECT_TRUE(ctx.Finalize());
}
