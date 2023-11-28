/**
 * This file is part of the CernVM File System.
 */

#include <pthread.h>

#include <cstdint>
#include <cstring>

#include <google/dense_hash_map>

#include "util/murmur.hxx"
#include "util/smalloc.h"

namespace compat {

template <typename hashed_type>
struct hash_murmur {
  size_t operator() (const hashed_type key) const {
#ifdef __x86_64__
    return MurmurHash64A(&key, sizeof(key), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&key, sizeof(key), 0x07387a4f);
#endif
  }
};

template<unsigned char StackSize, char Type>
struct ShortStringV1 {
  ~ShortStringV1() { delete long_string_; }

  const char *GetChars() const {
    if (long_string_) {
      return long_string_->data();
    } else {
      return stack_;
    }
  }

  unsigned GetLength() const {
    if (long_string_)
      return long_string_->length();
    return length_;
  }

  std::string *long_string_;
  char stack_[StackSize + 1];
  unsigned char length_;
};  // class ShortStringV1
typedef ShortStringV1<200, 0> PathStringV1;
typedef ShortStringV1<25, 1> NameStringV1;
typedef ShortStringV1<25, 2> LinkStringV1;

template<class Item>
struct BigQueueV1 {
  ~BigQueueV1() { Dealloc(); }
  size_t size() const { return size_; }
  bool IsEmpty() const { return size_ == 0; }
  bool Peek(Item **item) const {
    if (IsEmpty())
      return false;
    *item = head_;
    return true;
  }
  size_t GetHeadOffset() const { return head_ - buffer_; }
  void FreeBuffer(Item *buf, const size_t nitems) {
    for (size_t i = 0; i < nitems; ++i)
      buf[i].~Item();

    if (buf)
      smunmap(buf);
  }
  void Dealloc() {
    FreeBuffer(buffer_, GetHeadOffset() + size_);
    buffer_ = NULL;
    head_ = NULL;
    capacity_ = 0;
    size_ = 0;
  }

  Item *buffer_;
  Item *head_;
  size_t size_;
  size_t capacity_;
};  // class BigQueueV1

struct DirectoryListingV1 {
  char *buffer;  /**< Filled by fuse_add_direntry */
  size_t size;
  size_t capacity;
};

class DirectoryHandlesV1 : public google::dense_hash_map<uint64_t,
                                                         DirectoryListingV1,
                                                         hash_murmur<uint64_t> >
{};

struct InodeGenerationInfoV1 {
  unsigned version;
  uint64_t initial_revision;
  uint32_t incarnation;
  uint32_t overflow_counter;  // not used any more
  uint64_t inode_generation;
};

struct FuseStateV1 {
  unsigned version;
  bool cache_symlinks;
  bool has_dentry_expire;
};

struct DentryTrackerV1 {
  struct Entry {
    uint64_t expiry;
    uint64_t inode_parent;
    NameStringV1 name;
  };

  struct Statistics {
    int64_t num_insert;
    int64_t num_remove;
    int64_t num_prune;
  };

  static const unsigned kVersion = 0;

  ~DentryTrackerV1();
  void Lock() const;
  void Unlock() const;

  pthread_mutex_t *lock_;
  unsigned version_;
  Statistics statistics_;
  bool is_active_;
  BigQueueV1<Entry> entries_;

  int pipe_terminate_[2];
  int cleaning_interval_ms_;
  pthread_t thread_cleaner_;
};  // class DentryTrackerV1

} // namespace compat
