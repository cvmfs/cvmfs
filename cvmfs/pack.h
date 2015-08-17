/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PACK_H_
#define CVMFS_PACK_H_

#include <inttypes.h>

#include <cstdio>
#include <string>
#include <vector>

#include "gtest/gtest_prod.h"
#include "util.h"
#include "util_concurrency.h"

namespace shash {
class Any;
}

/**
 * Multiple content-addressable objects in a single BLOB.  A ObjectPack has
 * a header, an index containing all the objects and their offsets followed
 * by the concatenated objects.  The secure hash of the index is in the header.
 * Header and Index are JSON objects.
 *
 * This allows to verify the hash of the index and the hash of all objects
 * individually.  Thus, objects can be read and written in parallel to and from
 * the ObjectPack.
 *
 * Objects are used by tentacles to send change sets to the octopus server as
 * well as by the stratum 0 to transfer object bulks to stratum 1s during
 * replication.
 */
class ObjectPack {
  friend class ObjectPackProducer;
  FRIEND_TEST(T_Pack, Bucket);

 private:
  class Bucket;

 public:
  typedef Bucket * BucketHandle;

  static const uint64_t kDefaultLimit = 200*1024*1024;  // 200MB

  ObjectPack() : limit_(kDefaultLimit), size_(0) { }
  ~ObjectPack();

  BucketHandle OpenBucket();
  void AddToBucket(const void *buf, const uint64_t size,
                   const BucketHandle handle);
  bool CommitBucket(const shash::Any &id, const BucketHandle handle);
  void DiscardBucket(const BucketHandle handle);

  void TransferBucket(const BucketHandle handle, ObjectPack *other);

 private:
  /**
   * Wrapper around memory to which data can be added.  The memory should
   * represent a piece of content-addressable storage.
   */
  struct Bucket : SingleCopy {
     Bucket();
     ~Bucket();
     void Add(const void *buf, const uint64_t buf_size);

    unsigned char *content;
    uint64_t size;
    uint64_t capacity;
    shash::Any id;
  };

  /**
   * Maximum size of this object pack.
   */
  uint64_t limit_;
  /**
   * Accumulated size of all committed buckets.
   */
  uint64_t size_;
  /**
   * Buckets that were requested but that are not yet committed
   */
  std::set<BucketHandle> open_buckets_;
  /**
   * Buckets that are committed to the object pack.
   */
  std::vector<BucketHandle> buckets_;
};


/**
 * Seriializes ObjectPacks.  It can also serialize a single large file as an
 * "object pack", which otherwise would need special treatment.
 */
class ObjectPackProducer {
 public:
  explicit ObjectPackProducer(ObjectPack *pack);
  ObjectPackProducer(const shash::Any &id, FILE *big_file);
  unsigned ProduceNext(const unsigned buf_size, unsigned char *buf);
  void GetDigest(shash::Any *hash);
  unsigned GetHeaderSize() { return header_.size(); }

 private:
  /**
   * Unused if big_file_ is used.
   */
  ObjectPack *pack_;

  /**
   * Unused if object pack is used.  Rewind before giving to ObjectPackProducer.
   */
  FILE *big_file_;

  /**
   * Keeps track of how many bytes have been produced.
   */
  uint64_t pos_;

  /**
   * Keeps track of the current index in pack_->buckets_
   */
  unsigned idx_;

  /**
   * Keeps track of the current position in pack_->buckets_[idx_]
   */
  unsigned pos_in_bucket_;

  /**
   * The header is created in the constructor.
   */
  std::string header_;
};


/**
 * Data structures required for the ObjectPackConsumer.  BuildEvent is a
 * template parameter for the Observable base class of ObjectPack and hence
 * moved into this base class.
 */
class ObjectPackConsumerBase {
 public:
  struct BuildEvent {
    shash::Any id;
    uint64_t size;
    unsigned buf_size;
    void *buf;
  };

  enum BuildState {
    kStateContinue = 0,
    kStateDone,
    kStateAbort,
  };
};


/**
 * Deserializes and ObjectPack created by ObjectPackProducer.  For every object
 * it calls all listeners with a BuildEvent parameter at least once for every
 * object.  For large objects, it calls the listeners multiple times.  It won't
 * verify the incoming data, this is up to the listeners handling the data.
 * The ObjectPackConsumer will verify the header digest, however.
 */
class ObjectPackConsumer : public ObjectPackConsumerBase
                         , Observable<ObjectPackConsumerBase::BuildEvent> {
 public:
  explicit ObjectPackConsumer(const shash::Any &expected_digest);
  BuildState ConsumeNext(const unsigned buf_size, const void *buf);

 private:
  shash::Any expected_digest_;
};

#endif  // CVMFS_PACK_H_
