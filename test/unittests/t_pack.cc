/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>

#include "pack.h"
#include "prng.h"
#include "smalloc.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


class ConsumerCallbacks {
 public:
  ConsumerCallbacks()
    : total_size(0), total_objects(0), this_size(0), verify_errors(0) { }

  void OnEvent(const ObjectPackConsumer::BuildEvent &event) {
    total_size += event.buf_size;
    this_size += event.buf_size;
    if (this_size == event.size) {
      this_size = 0;
      total_objects++;
    }
  }

  void Reset() {
    total_size = total_objects = this_size = 0;
    verify_errors = 0;
  }


  uint64_t total_size;
  uint64_t total_objects;
  uint64_t this_size;
  unsigned verify_errors;
};


class T_Pack : public ::testing::Test {
 protected:
  virtual void SetUp() {
    hash_null_ = shash::Any(shash::kSha1);
    hash_partial_ = shash::Any(shash::kRmd160);
    hash_partial_.suffix = shash::kSuffixPartial;

    ffoo_ = CreateTempFile("cvmfstest", 0600, "w+", &foo_path_);
    assert(ffoo_);
    foo_content_ = "0123456789abcxyz";
    size_t nbytes = fwrite(foo_content_.data(), 1, foo_content_.size(), ffoo_);
    assert(nbytes == foo_content_.size());
    int retval = fflush(ffoo_);
    assert(retval == 0);

    unsigned char buf[4096];
    memset(buf, 0, 4096);
    ObjectPack::BucketHandle handle_one = pack_of_three_.OpenBucket();
    ObjectPack::BucketHandle handle_two = pack_of_three_.OpenBucket();
    ObjectPack::BucketHandle handle_three = pack_of_three_.OpenBucket();
    pack_of_three_.AddToBucket(buf, 4096, handle_one);
    buf[0] = '1';
    pack_of_three_.AddToBucket(buf, 1, handle_three);
    EXPECT_TRUE(pack_of_three_.CommitBucket(hash_null_, handle_one));
    EXPECT_TRUE(pack_of_three_.CommitBucket(hash_partial_, handle_two));
    EXPECT_TRUE(pack_of_three_.CommitBucket(hash_null_, handle_three));
  }

  virtual void TearDown() {
    fclose(ffoo_);
    unlink(foo_path_.c_str());
  }

  void RealWorld() {
    const unsigned kMaxObject = 4 * 1024 * 1024;  // 4MB
    const unsigned kBufSize = 1 * 1024 * 1024;  // 1MB
    Prng prng;
    prng.InitLocaltime();
    unsigned char *obj_buf =
      reinterpret_cast<unsigned char *>(smalloc(kMaxObject));
    for (unsigned i = 0; i < kMaxObject; ++i)
      obj_buf[i] = prng.Next(256);

    bool has_space = true;
    do {
      ObjectPack::BucketHandle handle = pack_.OpenBucket();
      uint64_t size = prng.Next(kMaxObject +1);
      shash::Any obj_digest(shash::kMd5);
      shash::HashMem(obj_buf, size, &obj_digest);
      pack_.AddToBucket(obj_buf, size, handle);
      has_space = pack_.CommitBucket(obj_digest, handle);
    } while (has_space);
    free(obj_buf);

    // printf("produced %d objects of total size %d MB\n",
    //         pack_.GetNoObjects(), pack_.size()/(1024*1024));

    shash::Any digest(shash::kShake128);
    ObjectPackProducer producer(&pack_);
    producer.GetDigest(&digest);
    ObjectPackConsumer consumer(digest, producer.GetHeaderSize());
    ConsumerCallbacks callbacks;
    // TODO(jblomer) Verify objects
    consumer.RegisterListener(&ConsumerCallbacks::OnEvent, &callbacks);
    unsigned char *transfer_buf =
      reinterpret_cast<unsigned char *>(smalloc(kBufSize));
    unsigned nspace;
    unsigned nwritten;
    do {
      nspace = prng.Next(kBufSize + 1);
      nwritten = producer.ProduceNext(nspace, transfer_buf);
      consumer.ConsumeNext(nwritten, transfer_buf);
    } while (nspace == nwritten);
    free(transfer_buf);

    EXPECT_EQ(ObjectPackConsumer::kStateDone, consumer.ConsumeNext(0, NULL));
    EXPECT_EQ(pack_.GetNoObjects(), callbacks.total_objects);
    EXPECT_EQ(pack_.size(), callbacks.total_size);
  }

  ObjectPack pack_;
  ObjectPack pack_of_three_;
  shash::Any hash_null_;
  shash::Any hash_partial_;
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

  EXPECT_TRUE(pack_.CommitBucket(shash::Any(hash_null_), handle_one));
  EXPECT_EQ(1U, pack_.open_buckets_.size());
  ASSERT_EQ(1U, pack_.buckets_.size());
  EXPECT_EQ(2U, pack_.buckets_[0]->size);
  EXPECT_EQ(2U, pack_.size_);

  ObjectPack::BucketHandle handle_three = pack_.OpenBucket();
  EXPECT_EQ(2U, pack_.open_buckets_.size());
  EXPECT_EQ(1U, pack_.buckets_.size());
  pack_.AddToBucket(&buf, 1, handle_three);
  EXPECT_TRUE(pack_.CommitBucket(shash::Any(hash_null_), handle_three));
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
  EXPECT_TRUE(small_pack.CommitBucket(shash::Any(hash_null_), handle_one));

  ObjectPack::BucketHandle handle_two = small_pack.OpenBucket();
  small_pack.AddToBucket(&buf, 1, handle_two);
  small_pack.AddToBucket(&buf, 1, handle_two);
  EXPECT_FALSE(small_pack.CommitBucket(shash::Any(hash_null_), handle_two));
  small_pack.DiscardBucket(handle_two);

  ObjectPack::BucketHandle handle_three = small_pack.OpenBucket();
  small_pack.AddToBucket(&buf, 1, handle_three);
  EXPECT_TRUE(small_pack.CommitBucket(shash::Any(hash_null_), handle_three));

  // Fail due to too many objects
  ObjectPack::BucketHandle *handles;
  handles = reinterpret_cast<ObjectPack::BucketHandle *>(
    smalloc((ObjectPack::kMaxObjects + 1) * sizeof(ObjectPack::BucketHandle)));
  for (unsigned i = 0; i < ObjectPack::kMaxObjects; ++i) {
    handles[i] = pack_.OpenBucket();
    EXPECT_TRUE(pack_.CommitBucket(hash_null_, handles[i]));
  }
  handles[ObjectPack::kMaxObjects] = pack_.OpenBucket();
  EXPECT_FALSE(pack_.CommitBucket(hash_null_,
               handles[ObjectPack::kMaxObjects]));
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
  ObjectPackProducer producer(&pack_of_three_);
  const string expected_result = "V1\nS4097\nN3\n--\n" +
    hash_null_.ToString(true) + " 4096\n" +
    hash_partial_.ToString(true) + " 0\n" +
    hash_null_.ToString(true) + " 1\n" +
    string(4096, '\0') + string(1, '1');

  unsigned char out_buf[8192];
  unsigned nbytes = producer.ProduceNext(8192, out_buf);
  EXPECT_EQ(expected_result.size(), nbytes);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(out_buf), nbytes));

  ObjectPackProducer producer_two(&pack_of_three_);
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
  const string expected_result = "V1\nS0\nN0\n--\n";
  EXPECT_EQ(12U, producer.GetHeaderSize());
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
  ObjectPackProducer producer(hash_null_, ffoo_);
  const string expected_result =
    "V1\nS" + StringifyInt(foo_content_.size()) + "\nN1\n--\n" +
    hash_null_.ToString(true) + " " + StringifyInt(foo_content_.size()) + "\n" +
    foo_content_;

  unsigned char buf[4096];
  unsigned nbytes = producer.ProduceNext(4096, buf);
  EXPECT_EQ(expected_result.size(), nbytes);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), nbytes));

  ObjectPackProducer producer_two(hash_null_, ffoo_);
  unsigned pos = 0;
  do {
    nbytes = producer_two.ProduceNext(1, buf + pos);
    pos++;
  } while (nbytes == 1);
  pos--;
  EXPECT_EQ(expected_result.size(), pos);
  EXPECT_EQ(expected_result, string(reinterpret_cast<char *>(buf), pos));
}


TEST_F(T_Pack, Consumer) {
  shash::Any digest(shash::kShake128);
  ObjectPackProducer producer(&pack_of_three_);
  producer.GetDigest(&digest);
  ObjectPackConsumer consumer(digest, producer.GetHeaderSize());

  ConsumerCallbacks callbacks;
  consumer.RegisterListener(&ConsumerCallbacks::OnEvent, &callbacks);
  unsigned char buf[8192];
  unsigned nbytes = producer.ProduceNext(8192, buf);
  EXPECT_EQ(ObjectPackConsumer::kStateDone, consumer.ConsumeNext(nbytes, buf));
  EXPECT_EQ(3U, callbacks.total_objects);
  EXPECT_EQ(pack_of_three_.size(), callbacks.total_size);

  callbacks.Reset();
  ObjectPackConsumer consumer_two(digest, producer.GetHeaderSize());
  consumer_two.RegisterListener(&ConsumerCallbacks::OnEvent, &callbacks);
  for (unsigned i = 0; i < nbytes; ++i)
    consumer_two.ConsumeNext(1, buf + i);
  EXPECT_EQ(ObjectPackConsumer::kStateDone, consumer_two.ConsumeNext(0, NULL));
  EXPECT_EQ(3U, callbacks.total_objects);
  EXPECT_EQ(pack_of_three_.size(), callbacks.total_size);
}


TEST_F(T_Pack, ConsumerEmpty) {
  shash::Any digest(shash::kShake128);
  ObjectPackProducer producer(&pack_);
  producer.GetDigest(&digest);
  ObjectPackConsumer consumer(digest, producer.GetHeaderSize());

  ConsumerCallbacks callbacks;
  consumer.RegisterListener(&ConsumerCallbacks::OnEvent, &callbacks);
  unsigned char buf[4096];
  unsigned nbytes = producer.ProduceNext(4096, buf);
  EXPECT_EQ(ObjectPackConsumer::kStateDone, consumer.ConsumeNext(nbytes, buf));
  EXPECT_EQ(0U, callbacks.total_objects);
  EXPECT_EQ(0U, callbacks.total_size);

  ObjectPackConsumer consumer_two(digest, producer.GetHeaderSize());
  for (unsigned i = 0; i < nbytes; ++i)
    consumer_two.ConsumeNext(1, buf + i);
  EXPECT_EQ(ObjectPackConsumer::kStateDone, consumer_two.ConsumeNext(0, NULL));
}


TEST_F(T_Pack, RealWorld) {
  RealWorld();
}


TEST_F(T_Pack, RealWorldSlow) {
  for (unsigned i = 0; i < 64; ++i)
    RealWorld();
}
