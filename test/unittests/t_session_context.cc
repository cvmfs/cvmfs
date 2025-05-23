/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "repository_tag.h"
#include "session_context.h"
#include "util/posix.h"

namespace {
RepositoryTag TestRepositoryTag() { return RepositoryTag("tag_name", ""); }
}  // namespace

class SessionContextMocked : public upload::SessionContext {
 public:
  SessionContextMocked() : num_jobs_dispatched_(0), num_jobs_finished_(0) { }

  int num_jobs_dispatched_;
  int num_jobs_finished_;

 protected:
  virtual Future<bool> *DispatchObjectPack(ObjectPack *pack) {
    Future<bool> *ret = SessionContext::DispatchObjectPack(pack);
    num_jobs_dispatched_++;
    return ret;
  }

  virtual bool Commit(const std::string & /*old_catalog*/,
                      const std::string & /*new_catalog*/,
                      const RepositoryTag & /*tag_name*/) {
    return true;
  }

  virtual bool DoUpload(const UploadJob * /*job*/) {
    num_jobs_finished_++;
    return true;
  }
};

class T_SessionContext : public ::testing::Test { };

TEST_F(T_SessionContext, BasicLifeCycle) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret"));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  ObjectPack::BucketHandle hd = ctx.NewBucket();

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  EXPECT_EQ(1, ctx.num_jobs_dispatched_);

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(1, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, MultipleFiles) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 20000));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, ""));
  }
  EXPECT_EQ(2, ctx.num_jobs_dispatched_);

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(3, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, MultipleFilesQueueSizeOne) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 5000, 1));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, ""));
  }
  EXPECT_EQ(9, ctx.num_jobs_dispatched_);

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(10, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, MultipleFilesForcedDispatchLast) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 20000));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    const bool dispatch = i == 9;
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", dispatch));
  }
  EXPECT_EQ(3, ctx.num_jobs_dispatched_);

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(3, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, MultipleFilesForcedDispatchEach) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 20000));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  for (int i = 0; i < 10; ++i) {
    ObjectPack::BucketHandle hd = ctx.NewBucket();

    unsigned char buffer[4096];
    memset(buffer, 0, 4096);
    ObjectPack::AddToBucket(buffer, 4096, hd);

    shash::Any hash(shash::kSha1);
    EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));
  }
  EXPECT_EQ(10, ctx.num_jobs_dispatched_);

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(10, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, FirstAddAllThenCommit) {
  SessionContextMocked ctx;
  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 20000));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

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

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(3, ctx.num_jobs_dispatched_);
  EXPECT_EQ(3, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, EncounterFileWhichIsLargerThanExpected) {
  SessionContextMocked ctx;

  EXPECT_TRUE(ctx.Initialize("http://my.repo.address:4929/api/v1",
                             "/path/to/the/session_file", "some_key_id",
                             "some_secret", 4000));
  EXPECT_EQ(0, ctx.num_jobs_dispatched_);
  EXPECT_EQ(0, ctx.num_jobs_finished_);

  ObjectPack::BucketHandle hd = ctx.NewBucket();

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  EXPECT_TRUE(ctx.CommitBucket(ObjectPack::kCas, hash, hd, "", true));

  EXPECT_TRUE(ctx.Finalize(true, "fake/old_root_hash", "fake/new_root_hash",
                           TestRepositoryTag()));
  EXPECT_EQ(1, ctx.num_jobs_dispatched_);
  EXPECT_EQ(1, ctx.num_jobs_finished_);
}

TEST_F(T_SessionContext, CurlUploadCallback) {
  ObjectPack pack(10000);

  ObjectPack::BucketHandle hd = pack.NewBucket();

  unsigned char buffer[4096];
  memset(buffer, 0, 4096);
  ObjectPack::AddToBucket(buffer, 4096, hd);

  shash::Any hash(shash::kSha1);
  shash::HashMem(buffer, 4096, &hash);
  pack.CommitBucket(ObjectPack::kCas, hash, hd, "");

  ObjectPackProducer serializer(&pack);

  const std::string message = "FAKE JSON MESSAGE";

  const size_t payload_size = message.size() + pack.size()
                              + serializer.GetHeaderSize();

  std::string text1;
  {
    ObjectPackProducer serializer2(&pack);
    std::vector<unsigned char> payload(0);
    std::vector<unsigned char> buffer(4096);
    unsigned nbytes = 0;
    do {
      nbytes = serializer2.ProduceNext(buffer.size(), &buffer[0]);
      std::copy(buffer.begin(), buffer.begin() + nbytes,
                std::back_inserter(payload));
    } while (nbytes > 0);
    text1 = message
            + std::string(reinterpret_cast<char *>(&payload[0]),
                          payload.size());
  }

  shash::Any hash1(shash::kSha1);
  shash::HashMem(reinterpret_cast<const unsigned char *>(text1.data()),
                 text1.size(), &hash1);
  const std::string digest1 = hash1.ToString(true);

  upload::CurlSendPayload payload;
  payload.json_message = &message;
  payload.pack_serializer = &serializer;
  payload.index = 0;

  size_t received_bytes = 0;
  size_t nbytes = 0;
  std::string output;
  do {
    char buffer[1024];
    nbytes = SendCB(buffer, 1024, 1, &payload);
    output += std::string(buffer, nbytes);
    received_bytes += nbytes;
  } while (nbytes > 0);

  shash::Any hash2(shash::kSha1);
  shash::HashMem(reinterpret_cast<const unsigned char *>(output.data()),
                 output.size(), &hash2);
  const std::string digest2 = hash2.ToString(true);

  EXPECT_EQ(digest1, digest2);

  EXPECT_EQ(payload_size, received_bytes);
}
