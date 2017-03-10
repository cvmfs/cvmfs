/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <session_context.h>

using namespace upload;

class SessionContextMocked : public SessionContext {
 public:
  SessionContextMocked() : num_jobs_dispatched_(0), num_jobs_finished_(0) {}

  int num_jobs_dispatched_;
  int num_jobs_finished_;

 protected:
  virtual Future<bool>* DispatchObjectPack(const ObjectPack* pack) {
    Future<bool>* ret = SessionContext::DispatchObjectPack(pack);
    num_jobs_dispatched_++;
    return ret;
  }

  virtual bool DoUpload(const UploadJob* /*job*/) {
    num_jobs_finished_++;
    return true;
  }
};

class T_SessionContext : public ::testing::Test {};

TEST_F(T_SessionContext, BasicLifeCycle) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file"));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 0);
  EXPECT_EQ(ctx.num_jobs_finished_, 0);

  ObjectPack::BucketHandle hd = ctx.NewBucket();

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 1);

  EXPECT_TRUE(ctx.Finalize());
  EXPECT_EQ(ctx.num_jobs_finished_, 1);
}

TEST_F(T_SessionContext, MultipleFiles) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file", false, 20000));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 0);
  EXPECT_EQ(ctx.num_jobs_finished_, 0);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, ""));
  }
  EXPECT_EQ(ctx.num_jobs_dispatched_, 2);

  EXPECT_TRUE(ctx.Finalize());
  EXPECT_EQ(ctx.num_jobs_finished_, 3);
}

TEST_F(T_SessionContext, MultipleFilesForcedDispatchLast) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file", false, 20000));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 0);
  EXPECT_EQ(ctx.num_jobs_finished_, 0);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    const bool dispatch = i == 9;
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", dispatch));
  }
  EXPECT_EQ(ctx.num_jobs_dispatched_, 3);

  EXPECT_TRUE(ctx.Finalize());
  EXPECT_EQ(ctx.num_jobs_finished_, 3);
}

TEST_F(T_SessionContext, MultipleFilesForcedDispatchEach) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file", false, 20000));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 0);
  EXPECT_EQ(ctx.num_jobs_finished_, 0);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  }
  EXPECT_EQ(ctx.num_jobs_dispatched_, 10);

  EXPECT_TRUE(ctx.Finalize());
  EXPECT_EQ(ctx.num_jobs_finished_, 10);
}

TEST_F(T_SessionContext, FirstAddAllThenCommit) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:8080/api/v1",
                             "/path/to/the/session_file", false, 20000));
  EXPECT_EQ(ctx.num_jobs_dispatched_, 0);
  EXPECT_EQ(ctx.num_jobs_finished_, 0);

  std::vector<ObjectPack::BucketHandle> hds(0);
  for (int i = 0; i < 10; ++i) {
    hds.push_back(ctx.NewBucket());

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hds.back());
  }

  for (int i = 0; i < 10; ++i) {
    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hds[i], ""));
  }

  EXPECT_TRUE(ctx.Finalize());
  EXPECT_EQ(ctx.num_jobs_dispatched_, 3);
  EXPECT_EQ(ctx.num_jobs_finished_, 3);
}
