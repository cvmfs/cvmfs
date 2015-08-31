/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/pack.h"


class T_Pack : public ::testing::Test {
 protected:
  virtual void SetUp() {
    hash_null = shash::Any(shash::kSha1);
  }

 protected:
  ObjectPack pack_;
  shash::Any hash_null;
};


TEST_F(T_Pack, Bucket) {
  ObjectPack::Bucket bucket;

  unsigned char buf[1024];
  bucket.Add(buf, 1);
  EXPECT_EQ(1U, bucket.size);
  bucket.Add(buf, 127);
  EXPECT_EQ(128U, bucket.size);
  bucket.Add(buf, 1);
  EXPECT_EQ(129U, bucket.size);
  bucket.Add(buf, 1024);
  EXPECT_EQ(1153U, bucket.size);
}


TEST_F(T_Pack, ObjectPack) {
  ObjectPack::BucketHandle handle_one = pack_.OpenBucket();
  ObjectPack::BucketHandle handle_two = pack_.OpenBucket();
  EXPECT_EQ(2U, pack_.open_buckets_.size());
  EXPECT_EQ(0U, pack_.buckets_.size());
  EXPECT_EQ(0U, pack_.size_);

  char buf = '0';
  pack_.AddToBucket(&buf, 1, handle_one);
  pack_.AddToBucket(&buf, 1, handle_one);

  EXPECT_TRUE(pack_.CommitBucket(shash::Any(hash_null), handle_one));
  EXPECT_EQ(1U, pack_.open_buckets_.size());
  ASSERT_EQ(1U, pack_.buckets_.size());
  EXPECT_EQ(2U, pack_.buckets_[0]->size);
  EXPECT_EQ(2U, pack_.size_);

  ObjectPack::BucketHandle handle_three = pack_.OpenBucket();
  EXPECT_EQ(2U, pack_.open_buckets_.size());
  EXPECT_EQ(1U, pack_.buckets_.size());
  pack_.AddToBucket(&buf, 1, handle_three);
  EXPECT_TRUE(pack_.CommitBucket(shash::Any(hash_null), handle_three));
  EXPECT_EQ(1U, pack_.open_buckets_.size());
  ASSERT_EQ(2U, pack_.buckets_.size());
  EXPECT_EQ(2U, pack_.buckets_[0]->size);
  EXPECT_EQ(1U, pack_.buckets_[1]->size);
  EXPECT_EQ(3U, pack_.size_);

  pack_.AddToBucket(&buf, 1, handle_two);
  pack_.DiscardBucket(handle_two);
  EXPECT_EQ(0U, pack_.open_buckets_.size());
  EXPECT_EQ(2U, pack_.buckets_.size());
}


TEST_F(T_Pack, ObjectPackOverflow) {
  ObjectPack small_pack(2);

  ObjectPack::BucketHandle handle_one = small_pack.OpenBucket();
  char buf = '0';
  small_pack.AddToBucket(&buf, 1, handle_one);
  EXPECT_TRUE(small_pack.CommitBucket(shash::Any(hash_null), handle_one));

  ObjectPack::BucketHandle handle_two = small_pack.OpenBucket();
  small_pack.AddToBucket(&buf, 1, handle_two);
  small_pack.AddToBucket(&buf, 1, handle_two);
  EXPECT_FALSE(small_pack.CommitBucket(shash::Any(hash_null), handle_two));

  ObjectPack::BucketHandle handle_three = small_pack.OpenBucket();
  small_pack.AddToBucket(&buf, 1, handle_three);
  EXPECT_TRUE(small_pack.CommitBucket(shash::Any(hash_null), handle_three));
}


TEST_F(T_Pack, ObjectPackTransfer) {
  ObjectPack other_pack;

  ObjectPack::BucketHandle handle = pack_.OpenBucket();
  EXPECT_EQ(1U, pack_.open_buckets_.size());
  EXPECT_EQ(0U, pack_.buckets_.size());

  pack_.TransferBucket(handle, &other_pack);
  EXPECT_EQ(0U, pack_.open_buckets_.size());
  EXPECT_EQ(0U, pack_.buckets_.size());
  EXPECT_EQ(1U, other_pack.open_buckets_.size());
  EXPECT_EQ(0U, other_pack.buckets_.size());
}
