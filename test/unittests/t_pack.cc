/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "../../cvmfs/pack.h"
#include "../../cvmfs/smalloc.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT


class T_Pack : public ::testing::Test {
 protected:
  virtual void SetUp() {
    hash_null = shash::Any(shash::kSha1);

    ffoo_ = CreateTempFile("cvmfstest", 0600, "w+", &foo_path_);
    assert(ffoo_);
    foo_content_ = "0123456789abcxyz";
    size_t nbytes = fwrite(foo_content_.data(), 1, foo_content_.size(), ffoo_);
    assert(nbytes == foo_content_.size());
    int retval = fflush(ffoo_);
    assert(retval == 0);
  }

  virtual void TearDown() {
    fclose(ffoo_);
    unlink(foo_path_.c_str());
  }

 protected:
  ObjectPack pack_;
  shash::Any hash_null;
  string foo_path_;
  FILE *ffoo_;
  string foo_content_;
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
  small_pack.DiscardBucket(handle_two);

  ObjectPack::BucketHandle handle_three = small_pack.OpenBucket();
  small_pack.AddToBucket(&buf, 1, handle_three);
  EXPECT_TRUE(small_pack.CommitBucket(shash::Any(hash_null), handle_three));

  // Fail due to too many objects
  ObjectPack::BucketHandle *handles;
  handles = reinterpret_cast<ObjectPack::BucketHandle *>(
    smalloc((ObjectPack::kMaxObjects + 1) * sizeof(ObjectPack::BucketHandle)));
  for (unsigned i = 0; i < ObjectPack::kMaxObjects; ++i) {
    handles[i] = pack_.OpenBucket();
    EXPECT_TRUE(pack_.CommitBucket(hash_null, handles[i]));
  }
  handles[ObjectPack::kMaxObjects] = pack_.OpenBucket();
  EXPECT_FALSE(pack_.CommitBucket(hash_null, handles[ObjectPack::kMaxObjects]));
  pack_.DiscardBucket(handles[ObjectPack::kMaxObjects]);
  free(handles);
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


TEST_F(T_Pack, Produce) {
  ObjectPack pack_of_three;
  unsigned char buf[4096];
  memset(buf, 0, 4096);
  ObjectPack::BucketHandle handle_one = pack_of_three.OpenBucket();
  ObjectPack::BucketHandle handle_two = pack_of_three.OpenBucket();
  ObjectPack::BucketHandle handle_three = pack_of_three.OpenBucket();
  pack_of_three.AddToBucket(buf, 4096, handle_one);
  buf[0] = '1';
  pack_of_three.AddToBucket(buf, 1, handle_three);
  EXPECT_TRUE(pack_of_three.CommitBucket(hash_null, handle_one));
  EXPECT_TRUE(pack_of_three.CommitBucket(hash_null, handle_two));
  EXPECT_TRUE(pack_of_three.CommitBucket(hash_null, handle_three));

  ObjectPackProducer producer(&pack_of_three);
  const string expected_result = "V 1\nS 4097\nN 3\n--\n" +
    hash_null.ToString() + " 0\n" +
    hash_null.ToString() + " 4096\n" +
    hash_null.ToString() + " 4096\n" +
    string(4096, '\0') + string(1, '1');

  unsigned char out_buf[8192];
  unsigned nbytes = producer.ProduceNext(8192, out_buf);
  EXPECT_EQ(expected_result.size(), nbytes);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(out_buf), nbytes));

  ObjectPackProducer producer_two(&pack_of_three);
  unsigned pos = 0;
  do {
    nbytes = producer_two.ProduceNext(1, out_buf + pos);
    pos++;
  } while (nbytes == 1);
  pos--;
  EXPECT_EQ(expected_result.size(), pos);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(out_buf), pos));
}


TEST_F(T_Pack, ProducerEmpty) {
  ObjectPackProducer producer(&pack_);
  const string expected_result = "V 1\nS 0\nN 0\n--\n";
  EXPECT_EQ(15U, producer.GetHeaderSize());
  shash::Any digest(shash::kSha1);
  producer.GetDigest(&digest);
  shash::Any expected_digest(shash::kSha1);
  shash::HashString(expected_result, &expected_digest);
  EXPECT_EQ(expected_digest, digest);

  unsigned char buf[4096];
  unsigned nbytes = producer.ProduceNext(4096, buf);
  EXPECT_EQ(expected_result.size(), nbytes);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), nbytes));

  ObjectPackProducer producer_two(&pack_);
  unsigned pos = 0;
  do {
    nbytes = producer_two.ProduceNext(1, buf + pos);
    pos++;
  } while (nbytes == 1);
  pos--;
  EXPECT_EQ(expected_result.size(), pos);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), pos));
}


TEST_F(T_Pack, ProducerFile) {
  ObjectPackProducer producer(hash_null, ffoo_);
  const string expected_result =
    "V 1\nS " + StringifyInt(foo_content_.size()) + "\nN 1\n--\n" +
    hash_null.ToString() + " 0\n" + foo_content_;

  unsigned char buf[4096];
  unsigned nbytes = producer.ProduceNext(4096, buf);
  EXPECT_EQ(expected_result.size(), nbytes);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), nbytes));

  ObjectPackProducer producer_two(hash_null, ffoo_);
  unsigned pos = 0;
  do {
    nbytes = producer_two.ProduceNext(1, buf + pos);
    pos++;
  } while (nbytes == 1);
  pos--;
  EXPECT_EQ(expected_result.size(), pos);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), pos));
}
