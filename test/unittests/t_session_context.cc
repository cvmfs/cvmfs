/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

using namespace upload;

class SessionContextMocked : public SessionContext {
  virtual bool DoUpload(const UploadJob* job) {
    job->result->Set(true);
    delete job->pack;
    delete job;
    return true;
  }
};

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, BasicLifeCycle) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file"));

  ObjectPack::BucketHandle hd = ctx.NewBucket();

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  EXPECT_TRUE(ctx.Finalize());
}
