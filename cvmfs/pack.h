/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PACK_H_
#define CVMFS_PACK_H_

#include <inttypes.h>
#include <pthread.h>

#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "gtest/gtest_prod.h"
#include "util/concurrency.h"
#include "util/single_copy.h"

/**
 * Multiple content-addressable objects in a single BLOB.  A (serialized)
 * ObjectPack has a header, an index containing all the objects and their
 * offsets followed by the concatenated objects.  The secure hash of the index
 * is in the header.
 *
 * This allows to verify the hash of the index and the hash of all objects
 * individually.  Thus, objects can be read and written in parallel to and from
 * the ObjectPack.
 *
 * Objects are used by "tentacles" to send change sets to the "octopus server"
 * as well as by the stratum 0 to transfer object bulks to stratum 1s during
 * replication.
 */
class ObjectPack : SingleCopy {
  FRIEND_TEST(T_Pack, Bucket);
  FRIEND_TEST(T_Pack, ObjectPack);
  FRIEND_TEST(T_Pack, ObjectPackTransfer);

 private:
  struct Bucket;

 public:
  typedef Bucket *BucketHandle;

  /**
   * This is used to identify the content type of different buckets. Initially,
   * the contents of a bucket are identified as kEmpty. When committing a
   * bucket, this is set to either kNamed - if the bucket holds the contents of
   * a named file - or kCas - if the bucket holds the contents of a content
   * addressable buffer.
   */
  enum BucketContentType {
    kEmpty,
    kNamed,
    kCas
  };

  static const uint64_t kDefaultLimit = 200 * 1024 * 1024;  // 200MB

  /**
   * Limit the maximum number of objects to avoid very large headers.  Assuming
   * Sha256 (71 bytes hex) + 9 bytes for the file sizes, a header with 100,000
   * files should fit in 10M.
   */
  static const uint64_t kMaxObjects = 100000;

  explicit ObjectPack(const uint64_t limit = kDefaultLimit);
  ~ObjectPack();

  static void AddToBucket(const void *buf, const uint64_t size,
                          const BucketHandle handle);

  BucketHandle NewBucket();

  bool CommitBucket(const BucketContentType type, const shash::Any &id,
                    const BucketHandle handle, const std::string &name = "");

  void DiscardBucket(const BucketHandle handle);
  void TransferBucket(const BucketHandle handle, ObjectPack *other);

  unsigned char *BucketContent(size_t idx) const;
  uint64_t BucketSize(size_t idx) const;
  const shash::Any &BucketId(size_t idx) const;

  uint64_t size() const { return size_; }

  // This returns the number of objects in the pack (equal to the number of
  // committed buckets)
  size_t GetNoObjects() const { return buckets_.size(); }

 private:
  /**
   * Wrapper around memory to which data can be added.  The memory should
   * represent a piece of content-addressable storage.
   */
  struct Bucket : SingleCopy {
    static const unsigned kInitialSize = 128;

    Bucket();
    ~Bucket();
    void Add(const void *buf, const uint64_t buf_size);

    unsigned char *content;
    uint64_t size;
    uint64_t capacity;
    shash::Any id;
    BucketContentType content_type;
    std::string name;
  };

  void InitLock();

  /**
   * Protects open_buckets_ and buckets_ collections.
   */
  pthread_mutex_t *lock_;

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
 * Data structures required for the ObjectPack serialization.  Event is a
 * template parameter for the Observable base class of ObjectPack and hence
 * moved into this base class.
 */
namespace ObjectPackBuild {
struct Event {
  Event(const shash::Any &id, uint64_t size, unsigned buf_size, const void *buf,
        ObjectPack::BucketContentType type, const std::string &name)
      : id(id)
      , size(size)
      , buf_size(buf_size)
      , buf(buf)
      , object_type(type)
      , object_name(name) { }

  shash::Any id;
  uint64_t size;
  unsigned buf_size;
  const void *buf;
  ObjectPack::BucketContentType object_type;
  std::string object_name;
};

enum State {
  kStateContinue = 0,
  kStateDone,
  kStateCorrupt,
  kStateBadFormat,
  kStateHeaderTooBig,
  kStateTrailingBytes,
};
}  // namespace ObjectPackBuild

/**
 * Serializes ObjectPacks.  It can also serialize a single large file as an
 * "object pack", which otherwise would need special treatment.
 *
 * The serialized format has a global, human readable header which has lines of
 * character keys and string values (like the cvmfs manifest) followed by a "--"
 * separator line followed by the index of objects. The index contains one line
 * for each item in the pack. Each line contains the following space-separated
 * tokens:
 * 1. object type identifier ('N' for named files, 'C' for CAS blobs)
 * 2. hash digest (hex)
 * 3. object size (decimal)
 * 4. object name - base64 encoding of the object name (optional - only if the
 *                  object type is 'N')
 */
class ObjectPackProducer {
 public:
  explicit ObjectPackProducer(ObjectPack *pack);
  ObjectPackProducer(const shash::Any &id, FILE *big_file,
                     const std::string &file_name = "");
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
  size_t idx_;

  /**
   * Keeps track of the current position in pack_->buckets_[idx_]
   */
  size_t pos_in_bucket_;

  /**
   * The header is created in the constructor.
   */
  std::string header_;
};

/**
 * Deserializes an ObjectPack created by ObjectPackProducer.  For every object
 * it calls all listeners with a Event parameter at least once for every
 * object.  For large objects, it calls the listeners multiple times.  It won't
 * verify the incoming data, this is up to the listeners handling the data.
 * The ObjectPackConsumer will verify the header digest, however.
 */
class ObjectPackConsumer : public Observable<ObjectPackBuild::Event> {
 public:
  explicit ObjectPackConsumer(const shash::Any &expected_digest,
                              const unsigned expected_header_size);
  ObjectPackBuild::State ConsumeNext(const unsigned buf_size,
                                     const unsigned char *buf);

 private:
  /**
   * For large objects, notify listeners in chunks of 128kB.
   */
  static const unsigned kAccuSize = 128 * 1024;

  struct IndexEntry {
    IndexEntry() : id(), size(), entry_type(), entry_name() { }
    IndexEntry(const shash::Any &id, const uint64_t size,
               ObjectPack::BucketContentType type, const std::string &name)
        : id(id), size(size), entry_type(type), entry_name(name) { }
    shash::Any id;
    uint64_t size;
    ObjectPack::BucketContentType entry_type;
    std::string entry_name;
  };

  bool ParseHeader();
  bool ParseItem(const std::string &line, IndexEntry *entry,
                 uint64_t *sum_size);

  ObjectPackBuild::State ConsumePayload(const unsigned buf_size,
                                        const unsigned char *buf);

  shash::Any expected_digest_;
  unsigned expected_header_size_;

  /**
   * Keeps track of how many bytes have been consumed from the payload.
   */
  uint64_t pos_;

  /**
   * Keeps track of the current index in the array of objects (index_)
   */
  unsigned idx_;

  /**
   * Keeps track of how many bytes have been processed from the current object.
   */
  unsigned pos_in_object_;

  /**
   * Collects data for large objects so that the number of callbacks to the
   * listeners is reduced.
   */
  unsigned char accumulator_[kAccuSize];

  /**
   * Keeps track of how many live bytes are stored in the accumulator_.
   */
  unsigned pos_in_accu_;

  /**
   * The state starts in kStateContinue and makes exactly one transition into
   * one of the other states as more bytes are consumed.
   */
  ObjectPackBuild::State state_;

  /**
   * Temporary store for the incomplete header.  Once completely consumed, the
   * header is interpreted into global_header_ and object_index_.
   */
  std::string raw_header_;

  /**
   * Total size of all the objects in the pack (header not included).
   */
  uint64_t size_;

  /**
   * Hash id and size of the individual objects in order.
   */
  std::vector<IndexEntry> index_;
};

#endif  // CVMFS_PACK_H_
