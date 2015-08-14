/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PACK_H_
#define CVMFS_PACK_H_

#include <inttypes.h>

#include <vector>

#include "util.h"

namespace shash {
class Any;
}

/**
 * Multiple content-addressable objects in a single BLOB.  An ObjectPack has
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
  class Bucket : SingleCopy {
   public:
     Bucket();
     ~Bucket();
     void Add(const void *buf, const uint64_t buf_size);
     void SetId(const shash::Any &id) { id_ = id; }
     uint64_t size() const { return size_; }
   private:
    unsigned char *content_;
    uint64_t size_;
    uint64_t capacity_;
    shash::Any id_;
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


class ObjectPackStreamer() {
  
};

#endif  // CVMFS_PACK_H_
